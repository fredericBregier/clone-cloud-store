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

package io.clonecloudstore.common.standard.system;

import java.util.Base64;
import java.util.concurrent.TimeUnit;

import com.google.common.io.BaseEncoding;
import io.quarkus.test.junit.QuarkusTest;
import org.apache.commons.codec.binary.Base16;
import org.apache.commons.codec.binary.Base32;
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
@Warmup(iterations = 3, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 3, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 1)
@BenchmarkMode(Mode.Throughput)
@Disabled("Bench only")
public class JMHBaseXxTestJmhIT {
  @State(Scope.Benchmark)
  public static class MyState {
    public byte[] bytes = RandomUtil.getRandom(1024);
    public Base32 base32 = new Base32(true);
    public Base16 base16 = new Base16(true);
    public org.apache.commons.codec.binary.Base64 base64 = new org.apache.commons.codec.binary.Base64();
    public BaseEncoding base32Guuava = BaseEncoding.base32Hex().lowerCase().omitPadding();
    public BaseEncoding base16Guuava = BaseEncoding.base16().lowerCase().omitPadding();
    public BaseEncoding base64Guava = BaseEncoding.base64().omitPadding();
    public Base64.Encoder base64Native = Base64.getEncoder().withoutPadding();
    public Base64.Encoder base64NativePadding = Base64.getEncoder();
    public Base64.Encoder base64NativeUrl = Base64.getUrlEncoder().withoutPadding();
  }

  @Benchmark
  public void testBase16(Blackhole blackhole, MyState myState) {
    blackhole.consume(BaseXx.getBase16(myState.bytes));
  }

  @Benchmark
  public void testBase16Apache(Blackhole blackhole, MyState myState) {
    blackhole.consume(myState.base16.encodeToString(myState.bytes).toLowerCase().replace("=", ""));
  }

  @Benchmark
  public void testBase16Guava(Blackhole blackhole, MyState myState) {
    blackhole.consume(myState.base16Guuava.encode(myState.bytes));
  }

  @Benchmark
  public void testBase32(Blackhole blackhole, MyState myState) {
    blackhole.consume(BaseXx.getBase32(myState.bytes));
  }

  @Benchmark
  public void testBase32Apache(Blackhole blackhole, MyState myState) {
    blackhole.consume(myState.base32.encodeToString(myState.bytes).toLowerCase().replace("=", ""));
  }

  @Benchmark
  public void testBase32Guava(Blackhole blackhole, MyState myState) {
    blackhole.consume(myState.base32Guuava.encode(myState.bytes));
  }

  @Benchmark
  public void testBase64(Blackhole blackhole, MyState myState) {
    blackhole.consume(BaseXx.getBase64(myState.bytes));
  }

  @Benchmark
  public void testBase64Padding(Blackhole blackhole, MyState myState) {
    blackhole.consume(BaseXx.getBase64Padding(myState.bytes));
  }

  @Benchmark
  public void testBase64Url(Blackhole blackhole, MyState myState) {
    blackhole.consume(BaseXx.getBase64Url(myState.bytes));
  }

  @Benchmark
  public void testBase64Native(Blackhole blackhole, MyState myState) {
    blackhole.consume(myState.base64Native.encodeToString(myState.bytes));
  }

  @Benchmark
  public void testBase64Guava(Blackhole blackhole, MyState myState) {
    blackhole.consume(myState.base64Guava.encode(myState.bytes));
  }

  @Benchmark
  public void testBase64Apache(Blackhole blackhole, MyState myState) {
    blackhole.consume(myState.base64.encodeToString(myState.bytes).replace("=", ""));
  }

  @Benchmark
  public void testBase64NativePadding(Blackhole blackhole, MyState myState) {
    blackhole.consume(myState.base64NativePadding.encodeToString(myState.bytes));
  }

  @Benchmark
  public void testBase64Native4Url(Blackhole blackhole, MyState myState) {
    blackhole.consume(myState.base64NativeUrl.encodeToString(myState.bytes));
  }

  public static void main(String[] args) throws Exception {
    Options opt = new OptionsBuilder().include(JMHBaseXxTestJmhIT.class.getSimpleName())
        //.addProfiler(StackProfiler.class)
        .addProfiler(GCProfiler.class).build();
    new Runner(opt).run();
  }

  @Test
  void runBenchmark() throws Exception {
    final var optionsBuilder = new OptionsBuilder().include(this.getClass().getName() + ".*").mode(Mode.Throughput)
        .warmupTime(TimeValue.seconds(1)).warmupIterations(3).threads(1).measurementIterations(3)
        .result("target/jmh-basexx-result-wall.csv").measurementTime(TimeValue.seconds(1))
        .timeUnit(TimeUnit.MICROSECONDS).forks(1).shouldFailOnError(true).resultFormat(ResultFormatType.CSV)
        .shouldDoGC(true);
    final var options = optionsBuilder.build();
    new Runner(options).run();
  }
}
