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

package org.apache.flink.examples.java.misc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

public class NativeOuterJoin {
	
	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.err.println("Usage: <leftInput> <rightInput> FULL|RIGHT|LEFT <outputPath>");
			System.exit(-1);
		}
		
		String leftInput = args[0];
		String rightInput = args[1];
		String outerType = args[2];
		String outputPath = args[3];
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().enableObjectReuse();
		
		CsvReader leftReader = env.readCsvFile(leftInput);
		leftReader.fieldDelimiter("|");
		leftReader.includeFields(true, true);
		DataSet<Tuple2<Integer, Integer>> left = leftReader.types(Integer.class, Integer.class);
		
		CsvReader rightReader = env.readCsvFile(rightInput);
		rightReader.fieldDelimiter("|");
		rightReader.includeFields(true, true);
		DataSet<Tuple2<Integer, String>> right = rightReader.types(Integer.class, String.class);
		
		DataSet<Tuple2<Integer, String>> result = null;
		if (outerType.toUpperCase().equals("FULL")) {
			result = left.fullOuterJoin(right).where(1).equalTo(0).with(new OuterJoinFlatJoinFunction());
		} else if (outerType.toUpperCase().equals("RIGHT")) {
			result = left.rightOuterJoin(right).where(1).equalTo(0).with(new OuterJoinFlatJoinFunction());
		} else if (outerType.toUpperCase().equals("LEFT")) {
			result = left.leftOuterJoin(right).where(1).equalTo(0).with(new OuterJoinFlatJoinFunction());
		} else {
			System.err.println("Usage: <leftInput> <rightInput> FULL|RIGHT|LEFT <outputPath>");
			System.exit(-1);
		}
		
		if (outputPath != null) {
			result.writeAsCsv(outputPath, FileSystem.WriteMode.OVERWRITE);
			env.execute("NativeOuterJoin Job");
		} else {
			result.print();
		}
	}
	
	private static class OuterJoinFlatJoinFunction implements org.apache.flink.api.common.functions.FlatJoinFunction<Tuple2<Integer, Integer>,
			Tuple2<Integer, String>, Tuple2<Integer, String>> {
		
		@Override
		public void join(
				Tuple2<Integer, Integer> first, Tuple2<Integer, String> second, Collector<Tuple2<Integer, String>> out
		) throws Exception {
			out.collect(new Tuple2<>(first == null ? -1 : first.f0, second == null ? "" : second.f1));
		}
	}
}
