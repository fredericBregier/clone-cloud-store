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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@QuarkusTest
class BaseXxTest {
  @Test
  void testBase16() throws IOException {
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> BaseXx.getBase16(null));
  }

  @Test
  void testBase32() throws FileNotFoundException {
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> BaseXx.getBase32(null));
  }

  @Test
  void testBase64() throws FileNotFoundException {
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> BaseXx.getBase64(null));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> BaseXx.getBase64Padding(null));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> BaseXx.getBase64Url(null));
  }

  @Test
  void testFromBase16() throws IOException {
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> BaseXx.getFromBase16(null));
  }

  @Test
  void testFromBase32() throws FileNotFoundException {
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> BaseXx.getFromBase32(null));
  }

  @Test
  void testFromBase64() throws FileNotFoundException {
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> BaseXx.getFromBase64(null));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> BaseXx.getFromBase64Padding(null));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> BaseXx.getFromBase64Url(null));
  }

  @Test
  void testBase64OK() throws IOException {
    final var encoded = BaseXx.getBase64("FBTest64P".getBytes());
    assertNotNull(encoded);
    final var bytes = BaseXx.getFromBase64(encoded);
    assertNotNull(bytes);
    assertArrayEquals(bytes, "FBTest64P".getBytes());
  }

  @Test
  void testBase64PaddingOK() throws IOException {
    final var encoded = BaseXx.getBase64Padding("FBTest64P".getBytes());
    assertNotNull(encoded);
    final var bytes = BaseXx.getFromBase64Padding(encoded);
    assertNotNull(bytes);
    assertArrayEquals(bytes, "FBTest64P".getBytes());
  }

  @Test
  void testBase64PaddingMixing1OK() throws IOException {
    final var encoded = BaseXx.getBase64Padding("FBTest64P".getBytes());
    assertNotNull(encoded);
    final var bytes = BaseXx.getFromBase64(encoded);
    assertNotNull(bytes);
    assertArrayEquals(bytes, "FBTest64P".getBytes());
  }

  @Test
  void testBase64PaddingMixing2OK() throws IOException {
    final var encoded = BaseXx.getBase64("FBTest64P".getBytes());
    assertNotNull(encoded);
    final var bytes = BaseXx.getFromBase64Padding(encoded);
    assertNotNull(bytes);
    assertArrayEquals(bytes, "FBTest64P".getBytes());
  }

  @Test
  void testBase64UrlOK() throws IOException {
    final var encoded = BaseXx.getBase64Url("FBTest64P".getBytes());
    assertNotNull(encoded);
    final var bytes = BaseXx.getFromBase64Url(encoded);
    assertNotNull(bytes);
    assertArrayEquals(bytes, "FBTest64P".getBytes());
  }

  @Test
  void testBase32OK() throws IOException {
    final var encoded = BaseXx.getBase32("FBTest32".getBytes());
    assertNotNull(encoded);
    final var bytes = BaseXx.getFromBase32(encoded);
    assertNotNull(bytes);
    assertArrayEquals(bytes, "FBTest32".getBytes());
  }

  @Test
  void testBase16OK() throws IOException {
    final var encoded = BaseXx.getBase16("FBTest16".getBytes());
    assertNotNull(encoded);
    final var bytes = BaseXx.getFromBase16(encoded);
    assertNotNull(bytes);
    assertArrayEquals(bytes, "FBTest16".getBytes());
  }

  @Test
  void testVariousBase16() {
    for (var i = 1; i < 100; i++) {
      final var bytes = RandomUtil.getRandom(i);
      final var base = BaseXx.getBase16(bytes);
      final var decoded = BaseXx.getFromBase16(base);
      assertArrayEquals(bytes, decoded);
    }
    for (var i = 5; i < 10; i++) {
      final var bytes = RandomUtil.getRandom(i);
      final var base = BaseXx.getBase16(bytes, 0, bytes.length);
      final var decoded = BaseXx.getFromBase16(base);
      assertArrayEquals(bytes, decoded);
      final var base2 = BaseXx.getBase16(bytes, 0, bytes.length - 1);
      final var decoded2 = BaseXx.getFromBase16(base2);
      assertArrayEquals(Arrays.copyOfRange(bytes, 0, bytes.length - 1), decoded2);
      final var base3 = BaseXx.getBase16(bytes, 1, bytes.length - 1);
      final var decoded3 = BaseXx.getFromBase16(base3);
      assertArrayEquals(Arrays.copyOfRange(bytes, 1, bytes.length), decoded3);
    }
  }

  @Test
  void testVariousBase32() {
    for (var i = 1; i < 100; i++) {
      final var bytes = RandomUtil.getRandom(i);
      final var base = BaseXx.getBase32(bytes);
      final var decoded = BaseXx.getFromBase32(base);
      assertArrayEquals(bytes, decoded);
    }
    for (var i = 5; i < 10; i++) {
      final var bytes = RandomUtil.getRandom(i);
      final var base = BaseXx.getBase32(bytes, 0, bytes.length);
      final var decoded = BaseXx.getFromBase32(base);
      assertArrayEquals(bytes, decoded);
      final var base2 = BaseXx.getBase32(bytes, 0, bytes.length - 1);
      final var decoded2 = BaseXx.getFromBase32(base2);
      assertArrayEquals(Arrays.copyOfRange(bytes, 0, bytes.length - 1), decoded2);
      final var base3 = BaseXx.getBase32(bytes, 1, bytes.length - 1);
      final var decoded3 = BaseXx.getFromBase32(base3);
      assertArrayEquals(Arrays.copyOfRange(bytes, 1, bytes.length), decoded3);
    }
  }

  @Test
  void testVariousBase64() {
    for (var i = 1; i < 100; i++) {
      final var bytes = RandomUtil.getRandom(i);
      final var base = BaseXx.getBase64(bytes);
      final var decoded = BaseXx.getFromBase64(base);
      assertArrayEquals(bytes, decoded);
    }
    for (var i = 5; i < 10; i++) {
      final var bytes = RandomUtil.getRandom(i);
      final var base = BaseXx.getBase64(bytes, 0, bytes.length);
      final var decoded = BaseXx.getFromBase64(base);
      assertArrayEquals(bytes, decoded);
      final var base2 = BaseXx.getBase64(bytes, 0, bytes.length - 1);
      final var decoded2 = BaseXx.getFromBase64(base2);
      assertArrayEquals(Arrays.copyOfRange(bytes, 0, bytes.length - 1), decoded2);
      final var base3 = BaseXx.getBase64(bytes, 1, bytes.length - 1);
      final var decoded3 = BaseXx.getFromBase64(base3);
      assertArrayEquals(Arrays.copyOfRange(bytes, 1, bytes.length - 1), decoded3);
    }
  }

  @Test
  void testVariousBase64Padding() {
    for (var i = 1; i < 100; i++) {
      final var bytes = RandomUtil.getRandom(i);
      final var base = BaseXx.getBase64Padding(bytes);
      final var decoded = BaseXx.getFromBase64Padding(base);
      assertArrayEquals(bytes, decoded);
    }
    for (var i = 5; i < 10; i++) {
      final var bytes = RandomUtil.getRandom(i);
      final var base = BaseXx.getBase64Padding(bytes, 0, bytes.length);
      final var decoded = BaseXx.getFromBase64Padding(base);
      assertArrayEquals(bytes, decoded);
      final var base2 = BaseXx.getBase64Padding(bytes, 0, bytes.length - 1);
      final var decoded2 = BaseXx.getFromBase64Padding(base2);
      assertArrayEquals(Arrays.copyOfRange(bytes, 0, bytes.length - 1), decoded2);
      final var base3 = BaseXx.getBase64Padding(bytes, 1, bytes.length - 1);
      final var decoded3 = BaseXx.getFromBase64Padding(base3);
      assertArrayEquals(Arrays.copyOfRange(bytes, 1, bytes.length - 1), decoded3);
    }
  }

  @Test
  void testVariousBase64Url() {
    for (var i = 1; i < 100; i++) {
      final var bytes = RandomUtil.getRandom(i);
      final var base = BaseXx.getBase64Url(bytes);
      final var decoded = BaseXx.getFromBase64Url(base);
      assertArrayEquals(bytes, decoded);
    }
    for (var i = 5; i < 10; i++) {
      final var bytes = RandomUtil.getRandom(i);
      final var base = BaseXx.getBase64Url(bytes, 0, bytes.length);
      final var decoded = BaseXx.getFromBase64Url(base);
      assertArrayEquals(bytes, decoded);
      final var base2 = BaseXx.getBase64Url(bytes, 0, bytes.length - 1);
      final var decoded2 = BaseXx.getFromBase64Url(base2);
      assertArrayEquals(Arrays.copyOfRange(bytes, 0, bytes.length - 1), decoded2);
      final var base3 = BaseXx.getBase64Url(bytes, 1, bytes.length - 1);
      final var decoded3 = BaseXx.getFromBase64Url(base3);
      assertArrayEquals(Arrays.copyOfRange(bytes, 1, bytes.length - 1), decoded3);
    }
  }
}
