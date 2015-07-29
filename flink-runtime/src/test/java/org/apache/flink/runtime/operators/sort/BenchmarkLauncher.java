package org.apache.flink.runtime.operators.sort;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class BenchmarkLauncher {

	private BenchmarkLauncher(){}

	public static void main(String... args) throws Exception {
		Options opts = new OptionsBuilder()
				.include(OuterJoinBenchmark.class.getSimpleName())
				.warmupIterations(10)
				.measurementIterations(10)
				.jvmArgs("-server")
				.forks(1)
				.shouldDoGC(true)
				.build();

		new Runner(opts).run();
	}
}
