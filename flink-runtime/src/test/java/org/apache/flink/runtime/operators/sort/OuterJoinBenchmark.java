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

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeutils.record.RecordComparator;
import org.apache.flink.api.common.typeutils.record.RecordPairComparator;
import org.apache.flink.api.common.typeutils.record.RecordSerializer;
import org.apache.flink.api.java.record.functions.JoinFunction;
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
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.Assert;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class OuterJoinBenchmark {

	private static final int LEFT_INPUT_SIZE = 20000;
	private static final int RIGHT_INPUT_SIZE = 10000;

	// total memory
	private static final int MEMORY_SIZE = 1024 * 1024 * 16;
	private static final int PAGES_FOR_BNLJN = 2;

	// random seeds for the left and right input data generators
	private static final long SEED1 = 561349061987311L;

	private static final long SEED2 = 231434613412342L;

	// dummy abstract task
	private final AbstractInvokable parentTask = new DummyInvokable();

	private IOManager ioManager;
	private MemoryManager memoryManager;

	private Collector<Record> collector;
	private NonReusingMergeOuterJoinIterator<Record, Record, Record> genericOuterJoinIterator;
	private NonReusingMergeLeftOuterJoinIterator<Record, Record, Record> dedicatedLeftOuterJoinIterator;

	private final FlatJoinFunction matcher = new JoinFunction() {
		@Override
		public void join(Record value1, Record value2, Collector<Record> out) throws Exception {

		}
	};
	private RecordComparator comparator1;
	private RecordComparator comparator2;
	private Generator generator1;
	private Generator generator2;
	private TestData.GeneratorIterator gen1Iter;
	private TestData.GeneratorIterator gen2Iter;
	private TestData.ConstantValueIterator const1Iter;
	private TestData.ConstantValueIterator const2Iter;
	private ArrayList<MutableObjectIterator<Record>> inList1;
	private ArrayList<MutableObjectIterator<Record>> inList2;
	private MergeIterator<Record> input1;
	private MergeIterator<Record> input2;
	private RecordPairComparator pairComparator;
	private RecordSerializer serializer1;
	private RecordSerializer serializer2;

	@Setup(Level.Trial)
	public void setUp() throws Exception {
		this.memoryManager = new DefaultMemoryManager(MEMORY_SIZE, 1);
		this.ioManager = new IOManagerAsync();

		serializer1 = RecordSerializer.get();
		serializer2 = RecordSerializer.get();
		comparator1 = new RecordComparator(new int[]{0}, new Class[]{TestData.Key.class});
		comparator2 = new RecordComparator(new int[]{0}, new Class[]{TestData.Key.class});
		pairComparator = new RecordPairComparator(new int[]{0}, new int[]{0}, new Class[]{TestData.Key.class});

		this.memoryManager = new DefaultMemoryManager(MEMORY_SIZE, 1);
		this.ioManager = new IOManagerAsync();

		final int DUPLICATE_KEY = 13;

		generator1 = new Generator(SEED1, 500, 0.02f, 4096, KeyMode.SORTED_SPARSE, ValueMode.RANDOM_LENGTH, null);
		generator2 = new Generator(SEED2, 500, 0.02f, 2048, KeyMode.SORTED_SPARSE, ValueMode.RANDOM_LENGTH, null);

		gen1Iter = new TestData.GeneratorIterator(generator1, LEFT_INPUT_SIZE);
		gen2Iter = new TestData.GeneratorIterator(generator2, RIGHT_INPUT_SIZE);

		const1Iter = new TestData.ConstantValueIterator(DUPLICATE_KEY, "LEFT String for Duplicate Keys", 4000);
		const2Iter = new TestData.ConstantValueIterator(DUPLICATE_KEY, "RIGHT String for Duplicate Keys", 1000);

		inList1 = new ArrayList<MutableObjectIterator<Record>>();
		inList1.add(gen1Iter);
		inList1.add(const1Iter);

		inList2 = new ArrayList<MutableObjectIterator<Record>>();
		inList2.add(gen2Iter);
		inList2.add(const2Iter);

		input1 = new MergeIterator<Record>(inList1, comparator1.duplicate());
		input2 = new MergeIterator<Record>(inList2, comparator2.duplicate());

		collector = new DiscardingOutputCollector<Record>();
	}

	@Setup(Level.Invocation)
	public void prepareNextRound() throws Exception {
		genericOuterJoinIterator = new NonReusingMergeOuterJoinIterator<Record, Record, Record>(
						OuterJoinType.LEFT, input1, input2, serializer1, comparator1, serializer2, comparator2,
						pairComparator, this.memoryManager, this.ioManager, PAGES_FOR_BNLJN, this.parentTask);

		dedicatedLeftOuterJoinIterator = new NonReusingMergeLeftOuterJoinIterator<Record, Record, Record>(
				input1, input2, serializer1, comparator1, serializer2, comparator2,
				pairComparator, this.memoryManager, this.ioManager, PAGES_FOR_BNLJN, this.parentTask);

		genericOuterJoinIterator.open();
		dedicatedLeftOuterJoinIterator.open();
	}

	@TearDown(Level.Invocation)
	public void reset() throws Exception {
		genericOuterJoinIterator.close();
		dedicatedLeftOuterJoinIterator.close();

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
	}

	@TearDown(Level.Trial)
	public void tearDown() throws Exception {
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


	@Benchmark
	@BenchmarkMode(Mode.AverageTime)
	@OutputTimeUnit(TimeUnit.SECONDS)
	public void testLeftOuterJoinWithSameClassForAllTypes() throws Exception {
		while (genericOuterJoinIterator.callWithNextKey(matcher, collector));
	}

	@Benchmark
	@BenchmarkMode(Mode.AverageTime)
	@OutputTimeUnit(TimeUnit.SECONDS)
	public void testLeftOuterJoinWithDedicatedClass() throws Exception {
		while (dedicatedLeftOuterJoinIterator.callWithNextKey(matcher, collector));
	}
}
