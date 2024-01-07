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
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicBoolean;

import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class SystemPropertyUtilTest {
  private static final String OS_NAME = "os.name";
  private static final String OTHER = "other";
  private static final String KEY_TEST_WRONG_DATA = "keyTestWrongData";
  private static final String KEY_TEST = "keyTest";
  private static final String KEY_ITEST = "keyTestI";
  private static final String KEY_LTEST = "keyTestL";
  private static final String KEY_BTEST = "keyTestB";
  private static final String KEY_VALUE = "KeyValue";
  private static final String WRONG_VALUE = "<![CDATA[";
  private static final int KEY_IVALUE = 1;
  private static final long KEY_LVALUE = 2L;
  private static final boolean KEY_BVALUE = true;

  @Test
  final void testSystemPropertyDefault() {
    SystemPropertyUtil.refresh();
    assertNull(SystemPropertyUtil.get(StandardProperties.CCS_MACHINE_ID));
    SystemPropertyUtil.set(StandardProperties.CCS_MACHINE_ID, "AE1632");
    assertEquals("AE1632", StandardProperties.getCcsMachineId());
    SystemPropertyUtil.clear(StandardProperties.CCS_MACHINE_ID);
    SystemPropertyUtil.refresh();
    assertNull(StandardProperties.getCcsMachineId());
  }

  @Test
  final void testSystemPropertyString() {
    System.setProperty(KEY_TEST_WRONG_DATA, WRONG_VALUE);
    SystemPropertyUtil.refresh();
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> SystemPropertyUtil.getAndSet(KEY_TEST, null));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> SystemPropertyUtil.getAndSet(KEY_TEST, null));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> SystemPropertyUtil.set(KEY_TEST, null));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> SystemPropertyUtil.set(KEY_TEST, WRONG_VALUE));
    SystemPropertyUtil.set(KEY_TEST, KEY_VALUE);
    assertTrue(SystemPropertyUtil.contains(KEY_TEST));
    assertEquals(KEY_VALUE, SystemPropertyUtil.get(KEY_TEST));
    assertEquals(KEY_VALUE, SystemPropertyUtil.get(KEY_TEST, OTHER));
    assertEquals(KEY_VALUE, SystemPropertyUtil.getAndSet(KEY_TEST, OTHER));
    assertEquals(OTHER, SystemPropertyUtil.getAndSet(KEY_TEST + '2', OTHER));
    assertEquals(OTHER, SystemPropertyUtil.set(KEY_TEST + '2', OTHER));

    assertTrue(SystemPropertyUtil.contains(KEY_TEST_WRONG_DATA));
    assertNull(SystemPropertyUtil.get(KEY_TEST_WRONG_DATA, null));
    assertEquals(OTHER, SystemPropertyUtil.getAndSet(KEY_TEST_WRONG_DATA, OTHER));

    // Wrong type
    assertEquals(1, SystemPropertyUtil.get(KEY_TEST_WRONG_DATA, 1));
    assertEquals(1, SystemPropertyUtil.get(KEY_TEST_WRONG_DATA, 1L));
  }

  @Test
  final void testSystemPropertyBoolean() {
    SystemPropertyUtil.set(KEY_BTEST, KEY_BVALUE);
    assertTrue(SystemPropertyUtil.contains(KEY_BTEST));
    assertEquals(Boolean.toString(KEY_BVALUE), SystemPropertyUtil.get(KEY_BTEST));
    assertEquals(KEY_BVALUE, SystemPropertyUtil.get(KEY_BTEST, false));
    assertEquals(KEY_BVALUE, SystemPropertyUtil.getAndSet(KEY_BTEST, false));
    assertFalse(SystemPropertyUtil.getAndSet(KEY_BTEST + '2', false));
    assertFalse(SystemPropertyUtil.set(KEY_BTEST + '2', false));
    assertFalse(SystemPropertyUtil.get(KEY_BTEST + '3', false));
    assertNull(SystemPropertyUtil.set(KEY_BTEST + '3', "true"));
    assertTrue(SystemPropertyUtil.get(KEY_BTEST + '3', false));
    assertEquals("true", SystemPropertyUtil.set(KEY_BTEST + '3', "yes"));
    assertTrue(SystemPropertyUtil.get(KEY_BTEST + '3', false));
    assertEquals("yes", SystemPropertyUtil.set(KEY_BTEST + '3', "1"));
    assertTrue(SystemPropertyUtil.get(KEY_BTEST + '3', false));
    assertEquals("1", SystemPropertyUtil.set(KEY_BTEST + '3', "yes2"));
    assertFalse(SystemPropertyUtil.get(KEY_BTEST + '3', false));
    assertEquals("yes2", SystemPropertyUtil.set(KEY_BTEST + '3', ""));
    assertTrue(SystemPropertyUtil.get(KEY_BTEST + '3', false));
  }

  @Test
  final void testSystemPropertyInt() {
    SystemPropertyUtil.set(KEY_ITEST, KEY_IVALUE);
    assertTrue(SystemPropertyUtil.contains(KEY_ITEST));
    assertEquals(Integer.toString(KEY_IVALUE), SystemPropertyUtil.get(KEY_ITEST));
    assertEquals(KEY_IVALUE, SystemPropertyUtil.get(KEY_ITEST, 4));
    assertEquals(KEY_IVALUE, SystemPropertyUtil.getAndSet(KEY_ITEST, 4));
    assertEquals(4, SystemPropertyUtil.getAndSet(KEY_ITEST + '2', 4));
    assertEquals(4, SystemPropertyUtil.set(KEY_ITEST + '2', 4));
    assertEquals(5, SystemPropertyUtil.get(KEY_ITEST + '3', 5));
    assertNull(SystemPropertyUtil.set(KEY_ITEST + '3', "yes2"));
    assertEquals(6, SystemPropertyUtil.get(KEY_ITEST + '3', 6));
  }

  @Test
  final void testSystemPropertyLong() {
    SystemPropertyUtil.set(KEY_LTEST, KEY_LVALUE);
    assertTrue(SystemPropertyUtil.contains(KEY_LTEST));
    assertEquals(Long.toString(KEY_LVALUE), SystemPropertyUtil.get(KEY_LTEST));
    assertEquals(KEY_LVALUE, SystemPropertyUtil.get(KEY_LTEST, 3L));
    assertEquals(KEY_LVALUE, SystemPropertyUtil.getAndSet(KEY_LTEST, 3L));
    assertEquals(3L, SystemPropertyUtil.getAndSet(KEY_LTEST + '2', 3L));
    assertEquals(3L, SystemPropertyUtil.set(KEY_LTEST + '2', 3L));
    assertEquals(4L, SystemPropertyUtil.get(KEY_LTEST + '3', 4L));
    assertNull(SystemPropertyUtil.set(KEY_LTEST + '3', "yes2"));
    assertEquals(5L, SystemPropertyUtil.get(KEY_LTEST + '3', 5L));
  }

  @Test
  final void testSystemPropertyDebug() {
    final var bool = new AtomicBoolean(false);
    final var outputStream = new OutputStream() {
      @Override
      public void write(final int i) throws IOException {
        bool.set(true);
      }

      @Override
      public void write(final byte[] b) throws IOException {
        bool.set(true);
      }

      @Override
      public void write(final byte[] b, final int off, final int len) throws IOException {
        bool.set(true);
      }
    };
    final var out = new PrintStream(outputStream);
    SystemPropertyUtil.debug(out);
    assertTrue(bool.get());
    final var oldOut = System.out;
    System.setOut(out);
    bool.set(false);
    SystemPropertyUtil.debug();
    assertTrue(bool.get());
    System.setOut(oldOut);
  }

  @Test
  final void testSystemPropertyOs() {
    SystemPropertyUtil.get(OS_NAME);
    final var platform = SystemPropertyUtil.getOS();
    switch (platform) {
      case MAC -> {
        assertTrue(SystemPropertyUtil.isMac());
        assertFalse(SystemPropertyUtil.isWindows());
        assertFalse(SystemPropertyUtil.isUnix());
        assertFalse(SystemPropertyUtil.isSolaris());
      }
      case SOLARIS -> {
        assertFalse(SystemPropertyUtil.isMac());
        assertFalse(SystemPropertyUtil.isWindows());
        assertFalse(SystemPropertyUtil.isUnix());
        assertTrue(SystemPropertyUtil.isSolaris());
      }
      case UNIX -> {
        assertFalse(SystemPropertyUtil.isMac());
        assertFalse(SystemPropertyUtil.isWindows());
        assertTrue(SystemPropertyUtil.isUnix());
        assertFalse(SystemPropertyUtil.isSolaris());
      }
      case UNSUPPORTED -> {
        assertFalse(SystemPropertyUtil.isMac());
        assertFalse(SystemPropertyUtil.isWindows());
        assertFalse(SystemPropertyUtil.isUnix());
        assertFalse(SystemPropertyUtil.isSolaris());
      }
      case WINDOWS -> {
        assertFalse(SystemPropertyUtil.isMac());
        assertTrue(SystemPropertyUtil.isWindows());
        assertFalse(SystemPropertyUtil.isUnix());
        assertFalse(SystemPropertyUtil.isSolaris());
      }
      default -> {
      }
    }
  }

  @Test
  final void testSystemPropertyError() {
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> SystemPropertyUtil.contains(null));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> SystemPropertyUtil.get(null));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> SystemPropertyUtil.get(null, KEY_IVALUE));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> SystemPropertyUtil.get(null, KEY_BVALUE));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> SystemPropertyUtil.get(null, KEY_LVALUE));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> SystemPropertyUtil.getAndSet(null, KEY_VALUE));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> SystemPropertyUtil.getAndSet(null, KEY_BVALUE));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> SystemPropertyUtil.getAndSet(null, KEY_IVALUE));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> SystemPropertyUtil.getAndSet(null, KEY_LVALUE));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> SystemPropertyUtil.set(null, KEY_VALUE));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> SystemPropertyUtil.set(null, KEY_BVALUE));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> SystemPropertyUtil.set(null, KEY_IVALUE));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> SystemPropertyUtil.set(null, KEY_LVALUE));
  }
}
