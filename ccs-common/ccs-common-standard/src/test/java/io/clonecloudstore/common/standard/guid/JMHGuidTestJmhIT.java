/*
 * Copyright (c) 2022-2024. Clone Cloud Store (CCS), Contributors and Frederic Bregier
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed
 *  under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
 *  OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.clonecloudstore.common.standard.guid;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

@QuarkusTest
@Warmup(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 1)
@BenchmarkMode(Mode.Throughput)
@Disabled("Bench only")
public class JMHGuidTestJmhIT {
  @State(Scope.Benchmark)
  public static class MyState {
  }

  @Benchmark
  public void testLongUuid(Blackhole blackhole) {
    blackhole.consume(new LongUuid().getId());
  }

  @Benchmark
  public void testGuidLike(Blackhole blackhole) {
    blackhole.consume(new GuidLike().getId());
  }

  @Benchmark
  public void testGuidLikeStatic(Blackhole blackhole) {
    blackhole.consume(GuidLike.getGuid());
  }

  @Benchmark
  public void testNativeUuid(Blackhole blackhole) {
    blackhole.consume(UUID.randomUUID().toString());
  }

  public static void main(String[] args) throws Exception {
    Options opt = new OptionsBuilder().include(JMHGuidTestJmhIT.class.getSimpleName())
        //.addProfiler(StackProfiler.class)
        .addProfiler(GCProfiler.class).build();

    new Runner(opt).run();
  }

  @Test
  void runBenchmark() throws Exception {
    final var optionsBuilder = new OptionsBuilder().include(this.getClass().getName() + ".*").mode(Mode.Throughput)
        .warmupTime(TimeValue.seconds(1)).warmupIterations(5).threads(1).measurementIterations(5)
        .result("target/jmh-guid-result-wall.csv").measurementTime(TimeValue.seconds(1)).timeUnit(TimeUnit.MICROSECONDS)
        .forks(1).shouldFailOnError(true).resultFormat(ResultFormatType.CSV).shouldDoGC(true);
    final var options = optionsBuilder.build();
    new Runner(options).run();
  }
}
