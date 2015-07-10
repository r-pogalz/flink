/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.operators.sort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.typeutils.GenericPairComparator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.record.RecordComparator;
import org.apache.flink.api.common.typeutils.record.RecordPairComparator;
import org.apache.flink.api.common.typeutils.record.RecordSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memorymanager.DefaultMemoryManager;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.operators.sort.AbstractMergeOuterJoinIterator.OuterJoinType;
import org.apache.flink.runtime.operators.testutils.DiscardingOutputCollector;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.runtime.operators.testutils.TestData.Generator;
import org.apache.flink.runtime.operators.testutils.TestData.Generator.KeyMode;
import org.apache.flink.runtime.operators.testutils.TestData.Generator.ValueMode;
import org.apache.flink.runtime.util.ResettableMutableObjectIterator;
import org.apache.flink.types.Record;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NonReusingSortMergeOuterJoinIteratorITCase {

	// total memory
	private static final int MEMORY_SIZE = 1024 * 1024 * 16;
	private static final int PAGES_FOR_BNLJN = 2;

	// the size of the left and right inputs
	private static final int INPUT_1_SIZE = 20000;

	private static final int INPUT_2_SIZE = 1000;

	// random seeds for the left and right input data generators
	private static final long SEED1 = 561349061987311L;

	private static final long SEED2 = 231434613412342L;

	// dummy abstract task
	private final AbstractInvokable parentTask = new DummyInvokable();

	private IOManager ioManager;
	private MemoryManager memoryManager;

	private TupleTypeInfo<Tuple2<String, String>> typeInfo1;
	private TupleTypeInfo<Tuple2<String, Integer>> typeInfo2;
	private TupleSerializer<Tuple2<String, String>> serializer1;
	private TupleSerializer<Tuple2<String, Integer>> serializer2;
	private TypeComparator<Tuple2<String, String>> comparator1;
	private TypeComparator<Tuple2<String, Integer>> comparator2;
	private TypePairComparator<Tuple2<String, String>, Tuple2<String, Integer>> pairComp;


	@Before
	public void beforeTest() {
		ExecutionConfig config = new ExecutionConfig();
		config.disableObjectReuse();

		typeInfo1 = TupleTypeInfo.getBasicTupleTypeInfo(String.class, String.class);
		typeInfo2 = TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class);
		serializer1 = typeInfo1.createSerializer(config);
		serializer2 = typeInfo2.createSerializer(config);
		comparator1 = typeInfo1.createComparator(new int[]{0}, new boolean[]{true}, 0, config);
		comparator2 = typeInfo2.createComparator(new int[]{0}, new boolean[]{true}, 0, config);
		pairComp = new GenericPairComparator<Tuple2<String, String>, Tuple2<String, Integer>>(comparator1, comparator2);

		this.memoryManager = new DefaultMemoryManager(MEMORY_SIZE, 1);
		this.ioManager = new IOManagerAsync();
	}

	@After
	public void afterTest() {
		if (this.ioManager != null) {
			this.ioManager.shutdown();
			if (!this.ioManager.isProperlyShutDown()) {
				Assert.fail("I/O manager failed to properly shut down.");
			}
			this.ioManager = null;
		}

		if (this.memoryManager != null) {
			Assert.assertTrue("Memory Leak: Not all memory has been returned to the memory manager.",
					this.memoryManager.verifyEmpty());
			this.memoryManager.shutdown();
			this.memoryManager = null;
		}
	}

	@Test
	public void testFullOuterWithSample() throws Exception {
		CollectionIterator<Tuple2<String, String>> input1 = createIterator(
				new Tuple2<String, String>("Jack", "Engineering"),
				new Tuple2<String, String>("Tim", "Sales"),
				new Tuple2<String, String>("Zed", "HR")
		);
		CollectionIterator<Tuple2<String, Integer>> input2 = createIterator(
				new Tuple2<String, Integer>("Allison", 100),
				new Tuple2<String, Integer>("Jack", 200),
				new Tuple2<String, Integer>("Zed", 150),
				new Tuple2<String, Integer>("Zed", 250)
		);

		OuterJoinType outerJoinType = OuterJoinType.FULL;
		List<Tuple4<String, String, String, Object>> actual = computeOuterJoin(input1, input2, outerJoinType);

		List<Tuple4<String, String, String, Object>> expected = Arrays.asList(
				new Tuple4<String, String, String, Object>(null, null, "Allison", 100),
				new Tuple4<String, String, String, Object>("Jack", "Engineering", "Jack", 200),
				new Tuple4<String, String, String, Object>("Tim", "Sales", null, null),
				new Tuple4<String, String, String, Object>("Zed", "HR", "Zed", 150),
				new Tuple4<String, String, String, Object>("Zed", "HR", "Zed", 250)
		);

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testLeftOuterWithSample() throws Exception {
		CollectionIterator<Tuple2<String, String>> input1 = createIterator(
				new Tuple2<String, String>("Jack", "Engineering"),
				new Tuple2<String, String>("Tim", "Sales"),
				new Tuple2<String, String>("Zed", "HR")
		);
		CollectionIterator<Tuple2<String, Integer>> input2 = createIterator(
				new Tuple2<String, Integer>("Allison", 100),
				new Tuple2<String, Integer>("Jack", 200),
				new Tuple2<String, Integer>("Zed", 150),
				new Tuple2<String, Integer>("Zed", 250)
		);

		List<Tuple4<String, String, String, Object>> actual = computeOuterJoin(input1, input2, OuterJoinType.LEFT);

		List<Tuple4<String, String, String, Object>> expected = Arrays.asList(
				new Tuple4<String, String, String, Object>("Jack", "Engineering", "Jack", 200),
				new Tuple4<String, String, String, Object>("Tim", "Sales", null, null),
				new Tuple4<String, String, String, Object>("Zed", "HR", "Zed", 150),
				new Tuple4<String, String, String, Object>("Zed", "HR", "Zed", 250)
		);

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testRightOuterWithSample() throws Exception {
		CollectionIterator<Tuple2<String, String>> input1 = createIterator(
				new Tuple2<String, String>("Jack", "Engineering"),
				new Tuple2<String, String>("Tim", "Sales"),
				new Tuple2<String, String>("Zed", "HR")
		);
		CollectionIterator<Tuple2<String, Integer>> input2 = createIterator(
				new Tuple2<String, Integer>("Allison", 100),
				new Tuple2<String, Integer>("Jack", 200),
				new Tuple2<String, Integer>("Zed", 150),
				new Tuple2<String, Integer>("Zed", 250)
		);

		List<Tuple4<String, String, String, Object>> actual = computeOuterJoin(input1, input2, OuterJoinType.RIGHT);

		List<Tuple4<String, String, String, Object>> expected = Arrays.asList(
				new Tuple4<String, String, String, Object>(null, null, "Allison", 100),
				new Tuple4<String, String, String, Object>("Jack", "Engineering", "Jack", 200),
				new Tuple4<String, String, String, Object>("Zed", "HR", "Zed", 150),
				new Tuple4<String, String, String, Object>("Zed", "HR", "Zed", 250)
		);

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testRightSideEmpty() throws Exception {
		CollectionIterator<Tuple2<String, String>> input1 = createIterator(
				new Tuple2<String, String>("Jack", "Engineering"),
				new Tuple2<String, String>("Tim", "Sales"),
				new Tuple2<String, String>("Zed", "HR")
		);
		CollectionIterator<Tuple2<String, Integer>> input2 = createIterator();

		List<Tuple4<String, String, String, Object>> actualLeft = computeOuterJoin(input1, input2, OuterJoinType.LEFT);
		List<Tuple4<String, String, String, Object>> actualRight = computeOuterJoin(input1, input2, OuterJoinType.RIGHT);
		List<Tuple4<String, String, String, Object>> actualFull = computeOuterJoin(input1, input2, OuterJoinType.FULL);

		List<Tuple4<String, String, String, Object>> expected = Arrays.asList(
				new Tuple4<String, String, String, Object>("Jack", "Engineering", null, null),
				new Tuple4<String, String, String, Object>("Tim", "Sales", null, null),
				new Tuple4<String, String, String, Object>("Zed", "HR", null, null)
		);

		Assert.assertEquals(expected, actualLeft);
		Assert.assertEquals(expected, actualFull);
		Assert.assertEquals(Collections.<Tuple4<String,String,String,Object>>emptyList(), actualRight);
	}

	@Test
	public void testLeftSideEmpty() throws Exception {
		CollectionIterator<Tuple2<String, String>> input1 = createIterator();
		CollectionIterator<Tuple2<String, Integer>> input2 = createIterator(
				new Tuple2<String, Integer>("Allison", 100),
				new Tuple2<String, Integer>("Jack", 200),
				new Tuple2<String, Integer>("Zed", 150),
				new Tuple2<String, Integer>("Zed", 250)
		);

		List<Tuple4<String, String, String, Object>> actualLeft = computeOuterJoin(input1, input2, OuterJoinType.LEFT);
		List<Tuple4<String, String, String, Object>> actualRight = computeOuterJoin(input1, input2, OuterJoinType.RIGHT);
		List<Tuple4<String, String, String, Object>> actualFull = computeOuterJoin(input1, input2, OuterJoinType.FULL);

		List<Tuple4<String, String, String, Object>> expected = Arrays.asList(
				new Tuple4<String, String, String, Object>(null, null, "Allison", 100),
				new Tuple4<String, String, String, Object>(null, null, "Jack", 200),
				new Tuple4<String, String, String, Object>(null, null, "Zed", 150),
				new Tuple4<String, String, String, Object>(null, null, "Zed", 250)
		);

		Assert.assertEquals(Collections.<Tuple4<String,String,String,Object>>emptyList(), actualLeft);
		Assert.assertEquals(expected, actualRight);
		Assert.assertEquals(expected, actualFull);
	}

	private <T> CollectionIterator<T> createIterator(T... values) {
		return new CollectionIterator<T>(Arrays.asList(values));
	}

	private List<Tuple4<String, String, String, Object>> computeOuterJoin(ResettableMutableObjectIterator<Tuple2<String, String>> input1,
																		  ResettableMutableObjectIterator<Tuple2<String, Integer>> input2,
																		  OuterJoinType outerJoinType) throws Exception {
		input1.reset();
		input2.reset();
		NonReusingMergeOuterJoinIterator<Tuple2<String, String>, Tuple2<String, Integer>, Tuple4<String, String, String, Object>> iterator =
				new NonReusingMergeOuterJoinIterator<Tuple2<String, String>, Tuple2<String, Integer>, Tuple4<String, String, String, Object>>(
						outerJoinType, input1, input2, serializer1, comparator1, serializer2, comparator2,
						pairComp, this.memoryManager, this.ioManager, PAGES_FOR_BNLJN, this.parentTask);

		List<Tuple4<String, String, String, Object>> actual = new ArrayList<Tuple4<String, String, String, Object>>();
        ListCollector<Tuple4<String, String, String, Object>> collector = new ListCollector<Tuple4<String, String, String, Object>>(actual);
        while (iterator.callWithNextKey(new SimpleFlatJoinFunction(), collector)) ;
		iterator.close();

		return actual;
	}

	@Test
	public void testFullOuterJoinWithHighNumberOfCommonKeys() {
			testOuterJoinWithHighNumberOfCommonKeys(OuterJoinType.FULL, 200, 500, 2048, 0.02f, 200, 500, 2048, 0.02f);
	}

	@Test
	public void testLeftOuterJoinWithHighNumberOfCommonKeys() {
		testOuterJoinWithHighNumberOfCommonKeys(OuterJoinType.LEFT, 200, 10, 4096, 0.02f, 100, 4000, 2048, 0.02f);
	}

	@Test
	public void testRightOuterJoinWithHighNumberOfCommonKeys() {
		testOuterJoinWithHighNumberOfCommonKeys(OuterJoinType.RIGHT, 100, 10, 2048, 0.02f, 200, 4000, 4096, 0.02f);
	}

	public void testOuterJoinWithHighNumberOfCommonKeys(OuterJoinType outerJoinType, int input1Size, int input1Duplicates, int input1ValueLength,
														float input1KeyDensity, int input2Size, int input2Duplicates, int input2ValueLength, float input2KeyDensity) {
		TypeSerializer<Record> serializer1 = RecordSerializer.get();
		TypeSerializer<Record> serializer2 = RecordSerializer.get();
		TypeComparator<Record> comparator1 = new RecordComparator(new int[]{0}, new Class[]{TestData.Key.class});
		TypeComparator<Record> comparator2 = new RecordComparator(new int[]{0}, new Class[]{TestData.Key.class});
		TypePairComparator<Record, Record> pairComparator = new RecordPairComparator(new int[]{0}, new int[]{0}, new Class[]{TestData.Key.class});

		this.memoryManager = new DefaultMemoryManager(MEMORY_SIZE, 1);
		this.ioManager = new IOManagerAsync();

		final int DUPLICATE_KEY = 13;

		try {
			final TestData.Generator generator1 = new Generator(SEED1, 500, input1KeyDensity, input1ValueLength, KeyMode.SORTED_SPARSE, ValueMode.RANDOM_LENGTH, null);
			final TestData.Generator generator2 = new Generator(SEED2, 500, input2KeyDensity, input2ValueLength, KeyMode.SORTED_SPARSE, ValueMode.RANDOM_LENGTH, null);

			final TestData.GeneratorIterator gen1Iter = new TestData.GeneratorIterator(generator1, input1Size);
			final TestData.GeneratorIterator gen2Iter = new TestData.GeneratorIterator(generator2, input2Size);

			final TestData.ConstantValueIterator const1Iter = new TestData.ConstantValueIterator(DUPLICATE_KEY, "LEFT String for Duplicate Keys", input1Duplicates);
			final TestData.ConstantValueIterator const2Iter = new TestData.ConstantValueIterator(DUPLICATE_KEY, "RIGHT String for Duplicate Keys", input2Duplicates);

			final List<MutableObjectIterator<Record>> inList1 = new ArrayList<MutableObjectIterator<Record>>();
			inList1.add(gen1Iter);
			inList1.add(const1Iter);

			final List<MutableObjectIterator<Record>> inList2 = new ArrayList<MutableObjectIterator<Record>>();
			inList2.add(gen2Iter);
			inList2.add(const2Iter);

			MutableObjectIterator<Record> input1 = new MergeIterator<Record>(inList1, comparator1.duplicate());
			MutableObjectIterator<Record> input2 = new MergeIterator<Record>(inList2, comparator2.duplicate());

			// collect expected data
			final Map<TestData.Key, Collection<Match>> expectedMatchesMap = joinValues(
					collectData(input1),
					collectData(input2),
					outerJoinType);

			// re-create the whole thing for actual processing

			// reset the generators and iterators
			generator1.reset();
			generator2.reset();
			const1Iter.reset();
			const2Iter.reset();
			gen1Iter.reset();
			gen2Iter.reset();

			inList1.clear();
			inList1.add(gen1Iter);
			inList1.add(const1Iter);

			inList2.clear();
			inList2.add(gen2Iter);
			inList2.add(const2Iter);

			input1 = new MergeIterator<Record>(inList1, comparator1.duplicate());
			input2 = new MergeIterator<Record>(inList2, comparator2.duplicate());

			final FlatJoinFunction matcher = new MatchRemovingMatcher(outerJoinType, expectedMatchesMap);

			final Collector<Record> collector = new DiscardingOutputCollector<Record>();


			// we create this sort-merge iterator with little memory for the block-nested-loops fall-back to make sure it
			// needs to spill for the duplicate keys
			NonReusingMergeOuterJoinIterator<Record, Record, Record> iterator =
					new NonReusingMergeOuterJoinIterator<Record, Record, Record>(
							outerJoinType, input1, input2, serializer1, comparator1, serializer2, comparator2,
							pairComparator, this.memoryManager, this.ioManager, PAGES_FOR_BNLJN, this.parentTask);

			iterator.open();

			while (iterator.callWithNextKey(matcher, collector)) ;

			iterator.close();

			// assert that each expected match was seen
			for (Entry<TestData.Key, Collection<Match>> entry : expectedMatchesMap.entrySet()) {
				if (!entry.getValue().isEmpty()) {
					Assert.fail("Collection for key " + entry.getKey() + " is not empty");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("An exception occurred during the test: " + e.getMessage());
		}
	}


	// --------------------------------------------------------------------------------------------
	//                                    Utilities
	// --------------------------------------------------------------------------------------------


	/**
	 * custom function that simply joins two tuples and considers null cases
	 */
	private static class SimpleFlatJoinFunction implements FlatJoinFunction<Tuple2<String, String>, Tuple2<String, Integer>, Tuple4<String, String, String, Object>> {

		@Override
		public void join(Tuple2<String, String> first, Tuple2<String, Integer> second, Collector<Tuple4<String, String, String, Object>> out) throws Exception {
			if (first == null) {
				out.collect(new Tuple4<String, String, String, Object>(null, null, second.f0, second.f1));
			} else if (second == null) {
				out.collect(new Tuple4<String, String, String, Object>(first.f0, first.f1, null, null));
			} else {
				out.collect(new Tuple4<String, String, String, Object>(first.f0, first.f1, second.f0, second.f1));
			}
		}
	}

	private Map<TestData.Key, Collection<Match>> joinValues(
			Map<TestData.Key, Collection<TestData.Value>> leftMap,
			Map<TestData.Key, Collection<TestData.Value>> rightMap,
			OuterJoinType outerJoinType) {
		Map<TestData.Key, Collection<Match>> map = new HashMap<TestData.Key, Collection<Match>>();

		for (TestData.Key key : leftMap.keySet()) {
			Collection<TestData.Value> leftValues = leftMap.get(key);
			Collection<TestData.Value> rightValues = rightMap.get(key);

			if (outerJoinType == OuterJoinType.RIGHT && rightValues == null) {
				continue;
			}

			if (!map.containsKey(key)) {
				map.put(key, new ArrayList<Match>());
			}

			Collection<Match> joinedValues = map.get(key);

			for (TestData.Value leftValue : leftValues) {
				if (rightValues != null) {
					for (TestData.Value rightValue : rightValues) {
						joinedValues.add(new Match(leftValue, rightValue));
					}
				} else {
					joinedValues.add(new Match(leftValue, null));
				}
			}
		}

		if (outerJoinType == OuterJoinType.RIGHT || outerJoinType == OuterJoinType.FULL) {
			for (TestData.Key key : rightMap.keySet()) {
				Collection<TestData.Value> leftValues = leftMap.get(key);
				Collection<TestData.Value> rightValues = rightMap.get(key);

				if (leftValues != null) {
					continue;
				}

				if (!map.containsKey(key)) {
					map.put(key, new ArrayList<Match>());
				}

				Collection<Match> joinedValues = map.get(key);

				for (TestData.Value rightValue : rightValues) {
					joinedValues.add(new Match(null, rightValue));
				}
			}
		}

		return map;
	}


	private Map<TestData.Key, Collection<TestData.Value>> collectData(MutableObjectIterator<Record> iter)
			throws Exception {
		Map<TestData.Key, Collection<TestData.Value>> map = new HashMap<TestData.Key, Collection<TestData.Value>>();
		Record pair = new Record();

		while ((pair = iter.next(pair)) != null) {
			TestData.Key key = pair.getField(0, TestData.Key.class);

			if (!map.containsKey(key)) {
				map.put(new TestData.Key(key.getKey()), new ArrayList<TestData.Value>());
			}

			Collection<TestData.Value> values = map.get(key);
			values.add(new TestData.Value(pair.getField(1, TestData.Value.class).getValue()));
		}

		return map;
	}

	/**
	 * Private class used for storage of the expected matches in a hashmap.
	 */
	private static class Match {
		private final Value left;

		private final Value right;

		public Match(Value left, Value right) {
			this.left = left;
			this.right = right;
		}

		@Override
		public boolean equals(Object obj) {
			Match o = (Match) obj;
			if (left == null && o.left == null && right.equals(o.right)) {
				return true;
			} else if (right == null && o.right == null && left.equals(o.left)) {
				return true;
			} else {
				return this.left.equals(o.left) && this.right.equals(o.right);
			}
		}

		@Override
		public int hashCode() {
			if (left == null) {
				return right.hashCode();
			} else if (right == null) {
				return left.hashCode();
			} else {
				return this.left.hashCode() ^ this.right.hashCode();
			}
		}

		@Override
		public String toString() {
			return left + ", " + right;
		}
	}

	private static final class MatchRemovingMatcher implements FlatJoinFunction<Record, Record, Record> {
		private static final long serialVersionUID = 1L;

		private final Map<TestData.Key, Collection<Match>> toRemoveFrom;
		private OuterJoinType outerJoinType;

		protected MatchRemovingMatcher(OuterJoinType outerJoinType, Map<TestData.Key, Collection<Match>> map) {
			this.outerJoinType = outerJoinType;
			this.toRemoveFrom = map;
		}

		@Override
		public void join(Record rec1, Record rec2, Collector<Record> out) throws Exception {
			TestData.Key key = rec1 != null ? rec1.getField(0, TestData.Key.class) : rec2.getField(0, TestData.Key.class);
			TestData.Value value1 = rec1 != null ? rec1.getField(1, TestData.Value.class) : null;
			TestData.Value value2 = rec2 != null ? rec2.getField(1, TestData.Value.class) : null;

			Collection<Match> matches = this.toRemoveFrom.get(key);
			if (matches == null) {
				Assert.fail("Match " + key + " - " + value1 + ":" + value2 + " is unexpected for outer join of type '" + outerJoinType + "'.");
			}

			boolean contained = matches.remove(new Match(value1, value2));
			if (!contained) {
				Assert.fail("Outer join type '" + outerJoinType + "'. Produced match was not contained: " + key + " - " + value1 + ":" + value2);
			}
			if (matches.isEmpty()) {
				this.toRemoveFrom.remove(key);
			}
		}

	}

	private class CollectionIterator<T> implements ResettableMutableObjectIterator<T> {

		private final Collection<T> collection;
		private Iterator<T> iterator;

		public CollectionIterator(Collection<T> collection) {
			this.collection = collection;
			this.iterator = collection.iterator();
		}

		@Override
		public T next(T reuse) throws IOException {
			return next();
		}

		@Override
		public T next() throws IOException {
			if (!iterator.hasNext()) {
				return null;
			} else {
				return iterator.next();
			}
		}

		@Override
		public void reset() throws IOException {
			iterator = collection.iterator();
		}
	}
}
