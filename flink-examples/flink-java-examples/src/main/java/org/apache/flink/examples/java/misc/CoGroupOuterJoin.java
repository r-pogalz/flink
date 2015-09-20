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

import java.util.ArrayList;
import java.util.List;

public class CoGroupOuterJoin {
	
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

		DataSet<Tuple2<Integer, String>> result = left.coGroup(right).where(1).equalTo(0).with(
				new CoGroupOuterJoinFunction(
						outerType
				)
		);
		
		if (outputPath != null) {
			result.writeAsCsv(outputPath, FileSystem.WriteMode.OVERWRITE);
			env.execute("CoGroupOuterJoin Job");
		} else {
			result.print();
		}
	}
	
	private static class CoGroupOuterJoinFunction implements org.apache.flink.api.common.functions.CoGroupFunction<Tuple2<Integer, Integer>,
			Tuple2<Integer, String>, Tuple2<Integer, String>> {
		
		private final String outerJoinType;
		
		public CoGroupOuterJoinFunction(String outerType) {
			outerJoinType = outerType.toUpperCase();
		}
		
		@Override
		public void coGroup(
				Iterable<Tuple2<Integer, Integer>> first,
				Iterable<Tuple2<Integer, String>> second,
				Collector<Tuple2<Integer, String>> out
		) throws Exception {
			if (outerJoinType.equals("LEFT")) {
				List<Tuple2<Integer, String>> rightSide = null;
				boolean rightHadElements = false;
				for (Tuple2<Integer, Integer> left : first) {
					if (rightSide == null) {
						rightSide = new ArrayList<>();
						for (Tuple2<Integer, String> right : second) {
							rightHadElements = true;
							out.collect(new Tuple2<>(left == null ? -1 : left.f0, right == null ? "" : right.f1));
							rightSide.add(right.copy());
						}
					} else {
						for (Tuple2<Integer, String> right : rightSide) {
							out.collect(new Tuple2<>(left == null ? -1 : left.f0, right == null ? "" : right.f1));
						}
					}
					if (!rightHadElements) {
						out.collect(new Tuple2<>(left == null ? -1 : left.f0, ""));
					}
				}
			} else if (outerJoinType.equals("RIGHT")) {
				boolean leftHadElements = false;
				List<Tuple2<Integer, Integer>> leftSide = null;
				for (Tuple2<Integer, String> right : second) {
					if (leftSide == null) {
						leftSide = new ArrayList<>();
						for (Tuple2<Integer, Integer> left : first) {
							leftHadElements = true;
							out.collect(new Tuple2<>(left == null ? -1 : left.f0, right == null ? "" : right.f1));
							leftSide.add(left.copy());
						}
					} else {
						for (Tuple2<Integer, Integer> left : leftSide) {
							out.collect(new Tuple2<>(left == null ? -1 : left.f0, right == null ? "" : right.f1));
						}
					}
					if (!leftHadElements) {
						out.collect(new Tuple2<>(-1, right == null ? "" : right.f1));
					}
				}
			} else if (outerJoinType.equals("FULL")) {
				boolean leftHadElements = false;
				boolean rightHadElements = false;
				List<Tuple2<Integer, String>> rightSide = null;
				for (Tuple2<Integer, Integer> left : first) {
					leftHadElements = true;
					if (rightSide == null) {
						rightSide = new ArrayList<>();
						for (Tuple2<Integer, String> right : second) {
							rightHadElements = true;
							out.collect(new Tuple2<>(left == null ? -1 : left.f0, right == null ? "" : right.f1));
							rightSide.add(right.copy());
						}
					} else {
						for (Tuple2<Integer, String> right : rightSide) {
							out.collect(new Tuple2<>(left == null ? -1 : left.f0, right == null ? "" : right.f1));
						}
					}
					if (!rightHadElements) {
						out.collect(new Tuple2<>(left == null ? -1 : left.f0, ""));
					}
				}
				
				if (!leftHadElements) {
					for (Tuple2<Integer, String> right : second) {
						out.collect(new Tuple2<>(-1, right == null ? "" : right.f1));
					}
				}
			} else {
				System.err.println("Usage: <leftInput> <rightInput> FULL|RIGHT|LEFT <outputPath>");
				System.exit(-1);
			}
		}
	}
}
