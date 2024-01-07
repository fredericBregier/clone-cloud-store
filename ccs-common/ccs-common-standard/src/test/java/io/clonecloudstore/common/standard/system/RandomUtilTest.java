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

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.clonecloudstore.test.stream.VoidOutputStream;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class RandomUtilTest {
  private static final byte[] BYTES_0_LENGTH = {};

  @Test
  void testRandom() {
    final var byteArray0 = RandomUtil.getRandom(90);
    assertNotNull(byteArray0);
    final var byteArray1 = RandomUtil.getRandom(90);
    assertNotEquals(byteArray0, byteArray1);
    final var byteArray3 = RandomUtil.getRandom(0);
    assertArrayEquals(BYTES_0_LENGTH, byteArray3);
    final var byteArray4 = RandomUtil.getRandom(-10);
    assertArrayEquals(BYTES_0_LENGTH, byteArray4);
    assertNotEquals(RandomUtil.getRandom().nextDouble(), RandomUtil.getRandom().nextDouble());
  }

  @Test
  void testSecureRandom() {
    final var byteArray0 = SystemRandomSecure.getRandom(90);
    assertNotNull(byteArray0);
    final var byteArray1 = SystemRandomSecure.getRandom(90);
    assertNotEquals(byteArray0, byteArray1);
    final var byteArray3 = SystemRandomSecure.getRandom(0);
    assertArrayEquals(BYTES_0_LENGTH, byteArray3);
    final var byteArray4 = SystemRandomSecure.getRandom(-10);
    assertArrayEquals(BYTES_0_LENGTH, byteArray4);
    assertNotEquals(SystemRandomSecure.getSecureRandomSingleton().nextDouble(),
        SystemRandomSecure.getSecureRandomSingleton().nextDouble());
  }

  @Test
  void testSingletons() throws IOException {
    final var bytes = SingletonUtils.getSingletonByteArray();
    assertEquals(0, bytes.length);

    final List<RandomUtilTest> emptyList = SingletonUtils.singletonList();
    assertTrue(emptyList.isEmpty());
    assertEquals(0, emptyList.size());
    assertThrows(UnsupportedOperationException.class, () -> emptyList.add(this));
    assertTrue(emptyList.isEmpty());
    assertEquals(0, emptyList.size());
    assertThrows(UnsupportedOperationException.class, () -> emptyList.remove(0));
    assertTrue(emptyList.isEmpty());
    assertEquals(0, emptyList.size());

    final Set<RandomUtilTest> emptySet = SingletonUtils.singletonSet();
    assertTrue(emptySet.isEmpty());
    assertEquals(0, emptySet.size());
    assertThrows(UnsupportedOperationException.class, () -> emptySet.add(this));
    assertTrue(emptySet.isEmpty());
    assertEquals(0, emptySet.size());
    emptySet.remove(this);
    assertTrue(emptySet.isEmpty());
    assertEquals(0, emptySet.size());

    final Map<RandomUtilTest, RandomUtilTest> emptyMap = SingletonUtils.singletonMap();
    assertTrue(emptyMap.isEmpty());
    assertEquals(0, emptyMap.size());
    assertThrows(UnsupportedOperationException.class, () -> emptyMap.put(this, this));
    assertTrue(emptyMap.isEmpty());
    assertEquals(0, emptyMap.size());
    emptyMap.remove(this);
    assertTrue(emptyMap.isEmpty());
    assertEquals(0, emptyMap.size());
  }

  @Test
  void testSingletonStreams() throws IOException {
    final var emptyIS = SingletonUtils.singletonInputStream();
    final var buffer = new byte[10];
    assertEquals(0, emptyIS.available());
    assertEquals(0, emptyIS.skip(10));
    assertEquals(-1, emptyIS.read());
    assertEquals(-1, emptyIS.read(buffer));
    assertEquals(-1, emptyIS.read(buffer, 0, buffer.length));
    assertFalse(emptyIS.markSupported());
    emptyIS.close();

    // No error
    final OutputStream voidOS = new VoidOutputStream();
    voidOS.write(buffer);
    voidOS.write(1);
    voidOS.write(buffer, 0, buffer.length);
    voidOS.flush();
    voidOS.close();
  }
}
