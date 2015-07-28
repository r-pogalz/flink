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

package org.apache.flink.runtime.operators.testutils;

import org.apache.flink.api.java.record.functions.JoinFunction;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;
import org.junit.Assert;

import java.util.Collection;
import java.util.Map;


public final class MatchRemovingMatcher extends JoinFunction {
	private static final long serialVersionUID = 1L;

	private final Map<TestData.Key, Collection<Match>> toRemoveFrom;

	public MatchRemovingMatcher(Map<TestData.Key, Collection<Match>> map) {
		this.toRemoveFrom = map;
	}

	@Override
	public void join(Record rec1, Record rec2, Collector<Record> out) throws Exception {
		TestData.Key key = rec1 != null ? rec1.getField(0, TestData.Key.class) : rec2.getField(0, TestData.Key.class);
		TestData.Value value1 = rec1 != null ? rec1.getField(1, TestData.Value.class) : null;
		TestData.Value value2 = rec2 != null ? rec2.getField(1, TestData.Value.class) : null;

		Collection<Match> matches = this.toRemoveFrom.get(key);
		if (matches == null) {
			Assert.fail("Match " + key + " - " + value1 + ":" + value2 + " is unexpected.");
		}

		boolean contained = matches.remove(new Match(value1, value2));
		if (!contained) {
			Assert.fail("Produced match was not contained: " + key + " - " + value1 + ":" + value2);
		}
		if (matches.isEmpty()) {
			this.toRemoveFrom.remove(key);
		}
	}
}
