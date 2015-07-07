package org.apache.flink.api.common.operators.base;


import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Joiner;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.operators.BinaryOperatorInformation;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.util.Collector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("serial")
public class SMOuterJoinOperatorBaseTest implements Serializable {

	private final FlatJoinFunction<String, String, String> joiner = new FlatJoinFunction<String, String, String>() {
		@Override
		public void join(String first, String second, Collector<String> out) throws Exception {
			out.collect(Joiner.on(',').join(String.valueOf(first), String.valueOf(second)));
		}
	};

	@SuppressWarnings({"rawtypes", "unchecked"})
	private final SMOuterJoinOperatorBase<String, String, String, FlatJoinFunction<String, String, String>> baseOperator =
			new SMOuterJoinOperatorBase(joiner,
					new BinaryOperatorInformation(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
							BasicTypeInfo.STRING_TYPE_INFO), new int[0], new int[0], "TestJoiner");

	@Test
	public void testFullOuterJoinWithoutMatchingPartners() throws Exception {
		final List<String> leftInput = Arrays.asList("foo", "bar", "foobar");
		final List<String> rightInput = Arrays.asList("oof", "rab", "raboof");
		baseOperator.setOuterJoinType(SMOuterJoinOperatorBase.OuterJoinType.FULL);
		List<String> expected = Arrays.asList("bar,null", "foo,null", "foobar,null", "null,oof", "null,rab", "null,raboof");
		testOuterJoin(leftInput, rightInput, expected);
	}

	@Test
	public void testFullOuterJoinWithFullMatchingKeys() throws Exception {
		final List<String> leftInput = Arrays.asList("foo", "bar", "foobar");
		final List<String> rightInput = Arrays.asList("bar", "foobar", "foo");
		baseOperator.setOuterJoinType(SMOuterJoinOperatorBase.OuterJoinType.FULL);
		List<String> expected = Arrays.asList("bar,bar", "foo,foo", "foobar,foobar");
		testOuterJoin(leftInput, rightInput, expected);
	}

	@Test
	public void testFullOuterJoinWithEmptyLeftInput() throws Exception {
		final List<String> leftInput = Arrays.asList();
		final List<String> rightInput = Arrays.asList("foo", "bar", "foobar");
		baseOperator.setOuterJoinType(SMOuterJoinOperatorBase.OuterJoinType.FULL);
		List<String> expected = Arrays.asList("null,bar", "null,foo", "null,foobar");
		testOuterJoin(leftInput, rightInput, expected);
	}

	@Test
	public void testFullOuterJoinWithEmptyRightInput() throws Exception {
		final List<String> leftInput = Arrays.asList("foo", "bar", "foobar");
		final List<String> rightInput = Arrays.asList();
		baseOperator.setOuterJoinType(SMOuterJoinOperatorBase.OuterJoinType.FULL);
		List<String> expected = Arrays.asList("bar,null", "foo,null", "foobar,null");
		testOuterJoin(leftInput, rightInput, expected);
	}

	@Test
	public void testFullOuterJoinWithPartialMatchingKeys() throws Exception {
		final List<String> leftInput = Arrays.asList("foo", "bar", "foobar");
		final List<String> rightInput = Arrays.asList("bar", "foo", "barfoo");
		baseOperator.setOuterJoinType(SMOuterJoinOperatorBase.OuterJoinType.FULL);
		List<String> expected = Arrays.asList("bar,bar", "null,barfoo", "foo,foo", "foobar,null");
		testOuterJoin(leftInput, rightInput, expected);
	}

	@Test
	public void testFullOuterJoinBuildingCorrectCrossProducts() throws Exception {
		final List<String> leftInput = Arrays.asList("foo", "foo", "foo", "bar","bar", "foobar", "foobar");
		final List<String> rightInput = Arrays.asList("foo", "foo", "bar", "bar", "bar", "barfoo", "barfoo");
		baseOperator.setOuterJoinType(SMOuterJoinOperatorBase.OuterJoinType.FULL);
		List<String> expected = Arrays.asList("bar,bar", "bar,bar", "bar,bar", "bar,bar", "bar,bar", "bar,bar",
				"null,barfoo", "null,barfoo", "foo,foo", "foo,foo", "foo,foo", "foo,foo", "foo,foo", "foo,foo",
				"foobar,null", "foobar,null");
		testOuterJoin(leftInput, rightInput, expected);
	}

	@Test
	public void testLeftOuterJoin() throws Exception {
		final List<String> leftInput = Arrays.asList("foo", "foo", "foo", "bar","bar", "foobar", "foobar");
		final List<String> rightInput = Arrays.asList("foo", "foo", "bar", "bar", "bar", "barfoo", "barfoo");
		baseOperator.setOuterJoinType(SMOuterJoinOperatorBase.OuterJoinType.LEFT);
		List<String> expected = Arrays.asList("bar,bar", "bar,bar", "bar,bar", "bar,bar", "bar,bar", "bar,bar",
				"foo,foo", "foo,foo", "foo,foo", "foo,foo", "foo,foo", "foo,foo", "foobar,null", "foobar,null");
		testOuterJoin(leftInput, rightInput, expected);
	}

	@Test
	public void testRightOuterJoin() throws Exception {
		final List<String> leftInput = Arrays.asList("foo", "foo", "foo", "bar","bar", "foobar", "foobar");
		final List<String> rightInput = Arrays.asList("foo", "foo", "bar", "bar", "bar", "barfoo", "barfoo");
		baseOperator.setOuterJoinType(SMOuterJoinOperatorBase.OuterJoinType.RIGHT);
		List<String> expected = Arrays.asList("bar,bar", "bar,bar", "bar,bar", "bar,bar", "bar,bar", "bar,bar",
				"null,barfoo", "null,barfoo", "foo,foo", "foo,foo", "foo,foo", "foo,foo", "foo,foo", "foo,foo");
		testOuterJoin(leftInput, rightInput, expected);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testThatExceptionIsThrownForOuterJoinTypeNull() throws Exception {
		final List<String> leftInput = Arrays.asList("foo", "bar", "foobar");
		final List<String> rightInput = Arrays.asList("bar", "foobar", "foo");

		baseOperator.setOuterJoinType(null);
		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.disableObjectReuse();
		baseOperator.executeOnCollections(leftInput, rightInput, null, executionConfig);
	}

	private void testOuterJoin(List<String> leftInput, List<String> rightInput, List<String> expected) throws Exception {
		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.disableObjectReuse();
		List<String> resultSafe = baseOperator.executeOnCollections(leftInput, rightInput, null, executionConfig);
		executionConfig.enableObjectReuse();
		List<String> resultRegular = baseOperator.executeOnCollections(leftInput, rightInput, null, executionConfig);

		assertEquals(expected, resultSafe);
		assertEquals(expected, resultRegular);
	}

}