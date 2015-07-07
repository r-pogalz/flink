package org.apache.flink.api.common.operators.base;

import org.apache.commons.collections.ResettableIterator;
import org.apache.commons.collections.iterators.ListIteratorWrapper;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.CopyingListCollector;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.operators.BinaryOperatorInformation;
import org.apache.flink.api.common.operators.DualInputOperator;
import org.apache.flink.api.common.operators.util.ListKeyGroupedIterator;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.GenericPairComparator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.*;

/**
 * TODO: implement left, right and full outer join on a sort-merge based algorithm
 */
public class SMOuterJoinOperatorBase<IN1, IN2, OUT, FT extends FlatJoinFunction<IN1, IN2, OUT>> extends DualInputOperator<IN1, IN2, OUT, FT> {

	public enum OuterJoinType {LEFT, RIGHT, FULL}

	private OuterJoinType outerJoinType;

	public SMOuterJoinOperatorBase(UserCodeWrapper<FT> udf, BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo, int[] keyPositions1, int[] keyPositions2, String name) {
		super(udf, operatorInfo, keyPositions1, keyPositions2, name);
	}

	public SMOuterJoinOperatorBase(FT udf, BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo, int[] keyPositions1, int[] keyPositions2, String name) {
		super(new UserCodeObjectWrapper<FT>(udf), operatorInfo, keyPositions1, keyPositions2, name);
	}

	public SMOuterJoinOperatorBase(Class<? extends FT> udf, BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo, int[] keyPositions1, int[] keyPositions2, String name) {
		super(new UserCodeClassWrapper<FT>(udf), operatorInfo, keyPositions1, keyPositions2, name);
	}

	public void setOuterJoinType(OuterJoinType outerJoinType) {
		this.outerJoinType = outerJoinType;
	}

	@Override
	protected List<OUT> executeOnCollections(List<IN1> leftInput, List<IN2> rightInput, RuntimeContext runtimeContext, ExecutionConfig executionConfig) throws Exception {
		TypeInformation<IN1> leftInformation = getOperatorInfo().getFirstInputType();
		TypeInformation<IN2> rightInformation = getOperatorInfo().getSecondInputType();
		TypeInformation<OUT> outInformation = getOperatorInfo().getOutputType();

		TypeComparator<IN1> leftComparator = buildComparatorFor(0, executionConfig, leftInformation);
		TypeComparator<IN2> rightComparator = buildComparatorFor(1, executionConfig, rightInformation);

		TypeSerializer<IN1> leftSerializer = leftInformation.createSerializer(executionConfig);
		TypeSerializer<IN2> rightSerializer = rightInformation.createSerializer(executionConfig);

		OuterJoinListIterator<IN1, IN2> outerJoinIterator =
				new OuterJoinListIterator<IN1, IN2>(leftInput, leftSerializer, leftComparator,
						rightInput, rightSerializer, rightComparator, outerJoinType);

		// --------------------------------------------------------------------
		// Run UDF
		// --------------------------------------------------------------------
		FlatJoinFunction<IN1, IN2, OUT> function = userFunction.getUserCodeObject();

		FunctionUtils.setFunctionRuntimeContext(function, runtimeContext);
		FunctionUtils.openFunction(function, this.parameters);


		List<OUT> result = new ArrayList<OUT>();
		Collector<OUT> collector = new CopyingListCollector<OUT>(result, outInformation.createSerializer(executionConfig));

		while (outerJoinIterator.next()) {
			function.join(leftSerializer.copy(outerJoinIterator.getLeft()), rightSerializer.copy(outerJoinIterator.getRight()), collector);
		}

		return result;
	}

	private <T> TypeComparator<T> buildComparatorFor(int input, ExecutionConfig executionConfig, TypeInformation<T> typeInformation) {
		TypeComparator<T> comparator;
		if (typeInformation instanceof AtomicType) {
			comparator = ((AtomicType<T>) typeInformation).createComparator(true, executionConfig);
		} else if (typeInformation instanceof CompositeType) {
			int[] keyPositions = getKeyColumns(input);
			boolean[] orders = new boolean[keyPositions.length];
			Arrays.fill(orders, true);

			comparator = ((CompositeType<T>) typeInformation).createComparator(keyPositions, orders, 0, executionConfig);
		} else {
			throw new RuntimeException("Type information for input of type " + typeInformation.getClass()
					.getCanonicalName() + " is not supported. Could not generate a comparator.");
		}
		return comparator;
	}

	private static class OuterJoinListIterator<IN1, IN2> {


		private static enum MatchStatus {
			NONE_REMAINED, FIRST_REMAINED, SECOND_REMAINED, FIRST_EMPTY, SECOND_EMPTY;
		}

		private OuterJoinType outerJoinType;

		private final TypeSerializer<IN1> leftSerializer;
		private final TypeSerializer<IN2> rightSerializer;

		private ListKeyGroupedIterator<IN1> leftGroupedIterator;
		private ListKeyGroupedIterator<IN2> rightGroupedIterator;
		private Iterable<IN1> currLeftSubset;
		private ResettableIterator currLeftIterator;
		private Iterable<IN2> currRightSubset;
		private ResettableIterator currRightIterator;

		private MatchStatus matchStatus;
		private GenericPairComparator<IN1, IN2> pairComparator;
		private Iterator<IN1> leftIterator;
		private Iterator<IN2> rightIterator;

		private IN1 leftReturn;
		private IN2 rightReturn;

		public OuterJoinListIterator(List<IN1> leftInput, TypeSerializer<IN1> leftSerializer, final TypeComparator<IN1> leftComparator,
									 List<IN2> rightInput, TypeSerializer<IN2> rightSerializer, final TypeComparator<IN2> rightComparator,
									 OuterJoinType outerJoinType) {
			this.outerJoinType = outerJoinType;
			this.leftSerializer = leftSerializer;
			this.rightSerializer = rightSerializer;
			pairComparator = new GenericPairComparator<IN1, IN2>(leftComparator, rightComparator);
			leftGroupedIterator = new ListKeyGroupedIterator<IN1>(leftInput, leftSerializer, leftComparator);
			rightGroupedIterator = new ListKeyGroupedIterator<IN2>(rightInput, rightSerializer, rightComparator);
			// ----------------------------------------------------------------
			// Sort
			// ----------------------------------------------------------------
			Collections.sort(leftInput, new Comparator<IN1>() {
				@Override
				public int compare(IN1 o1, IN1 o2) {
					return leftComparator.compare(o1, o2);
				}
			});

			Collections.sort(rightInput, new Comparator<IN2>() {
				@Override
				public int compare(IN2 o1, IN2 o2) {
					return rightComparator.compare(o1, o2);
				}
			});

		}

		private boolean next() throws IOException {
			boolean hasMoreElements = false;
			if ((currLeftIterator == null || !currLeftIterator.hasNext()) && (currRightIterator == null || !currRightIterator.hasNext())) {
				hasMoreElements = nextGroups(outerJoinType);
				if (hasMoreElements) {
					if (outerJoinType != OuterJoinType.LEFT) {
						currLeftIterator = new ListIteratorWrapper(currLeftSubset.iterator());
					}
					leftReturn = (IN1) currLeftIterator.next();
					if (outerJoinType != OuterJoinType.RIGHT) {
						currRightIterator = new ListIteratorWrapper(currRightSubset.iterator());
					}
					rightReturn = (IN2) currRightIterator.next();
					return true;
				} else {
					//no more elements
					return false;
				}
			} else if (currLeftIterator.hasNext() && !currRightIterator.hasNext()) {
				leftReturn = (IN1) currLeftIterator.next();
				currRightIterator.reset();
				rightReturn = (IN2) currRightIterator.next();
				return true;
			} else {
				rightReturn = (IN2) currRightIterator.next();
				return true;
			}
		}

		private boolean nextGroups(OuterJoinType outerJoinType) throws IOException {
			if (outerJoinType == OuterJoinType.FULL) {
				return nextGroups();
			} else if (outerJoinType == OuterJoinType.LEFT) {
				boolean leftContainsElements = false;
				while (!leftContainsElements && nextGroups()) {
					currLeftIterator = new ListIteratorWrapper(currLeftSubset.iterator());
					if (currLeftIterator.next() != null) {
						leftContainsElements = true;
					}
					currLeftIterator.reset();
				}
				return leftContainsElements;
			} else if (outerJoinType == OuterJoinType.RIGHT) {
				boolean rightContainsElements = false;
				while (!rightContainsElements && nextGroups()) {
					currRightIterator = new ListIteratorWrapper(currRightSubset.iterator());
					if (currRightIterator.next() != null) {
						rightContainsElements = true;
					}
					currRightIterator.reset();
				}
				return rightContainsElements;
			} else {
				throw new IllegalArgumentException("Outer join of type '" + outerJoinType + "' not supported.");
			}
		}

		private boolean nextGroups() throws IOException {
			boolean firstEmpty = true;
			boolean secondEmpty = true;

			if (this.matchStatus != MatchStatus.FIRST_EMPTY) {
				if (this.matchStatus == MatchStatus.FIRST_REMAINED) {
					// comparator is still set correctly
					firstEmpty = false;
				} else {
					if (this.leftGroupedIterator.nextKey()) {
						this.pairComparator.setReference(leftGroupedIterator.getValues().getCurrent());
						firstEmpty = false;
					}
				}
			}

			if (this.matchStatus != MatchStatus.SECOND_EMPTY) {
				if (this.matchStatus == MatchStatus.SECOND_REMAINED) {
					secondEmpty = false;
				} else {
					if (rightGroupedIterator.nextKey()) {
						secondEmpty = false;
					}
				}
			}

			if (firstEmpty && secondEmpty) {
				// both inputs are empty
				return false;
			} else if (firstEmpty && !secondEmpty) {
				// input1 is empty, input2 not
				this.currLeftSubset = Collections.singleton(null);
				this.currRightSubset = this.rightGroupedIterator.getValues();
				this.matchStatus = MatchStatus.FIRST_EMPTY;
				return true;
			} else if (!firstEmpty && secondEmpty) {
				// input1 is not empty, input 2 is empty
				this.currLeftSubset = this.leftGroupedIterator.getValues();
				this.currRightSubset = Collections.singleton(null);
				this.matchStatus = MatchStatus.SECOND_EMPTY;
				return true;
			} else {
				// both inputs are not empty
				final int comp = this.pairComparator.compareToReference(rightGroupedIterator.getValues().getCurrent());

				if (0 == comp) {
					// keys match
					this.currLeftSubset = this.leftGroupedIterator.getValues();
					this.currRightSubset = this.rightGroupedIterator.getValues();
					this.matchStatus = MatchStatus.NONE_REMAINED;
				} else if (0 < comp) {
					// key1 goes first
					this.currLeftSubset = this.leftGroupedIterator.getValues();
					this.currRightSubset = Collections.singleton(null);
					this.matchStatus = MatchStatus.SECOND_REMAINED;
				} else {
					// key 2 goes first
					this.currLeftSubset = Collections.singleton(null);
					this.currRightSubset = this.rightGroupedIterator.getValues();
					this.matchStatus = MatchStatus.FIRST_REMAINED;
				}
				return true;
			}
		}

		private IN1 getLeft() {
			return leftReturn;
		}

		private IN2 getRight() {
			return rightReturn;
		}
	}
}
