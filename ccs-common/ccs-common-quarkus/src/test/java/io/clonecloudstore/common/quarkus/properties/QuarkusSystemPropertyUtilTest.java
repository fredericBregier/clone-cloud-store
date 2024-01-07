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

package io.clonecloudstore.common.quarkus.properties;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicBoolean;

import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class QuarkusSystemPropertyUtilTest {
  private static final String OS_NAME = "os.name";
  private static final String OTHER = "other";
  private static final String KEY_TEST = "keyTest";
  private static final String KEY_ITEST = "keyTestI";
  private static final String KEY_LTEST = "keyTestL";
  private static final String KEY_BTEST = "keyTestB";
  private static final String KEY_VALUE = "KeyValue";
  private static final int KEY_IVALUE = 1;
  private static final long KEY_LVALUE = 2L;
  private static final boolean KEY_BVALUE = true;

  @Test
  final void testSystemPropertyDefault() {
    QuarkusSystemPropertyUtil.refresh();
    assertNotNull(QuarkusSystemPropertyUtil.get(QuarkusProperties.CCS_MACHINE_ID));
    QuarkusSystemPropertyUtil.set(QuarkusProperties.CCS_MACHINE_ID, "AE1632");
    assertEquals("AE1632", QuarkusProperties.getCcsMachineId());
    QuarkusSystemPropertyUtil.clear(QuarkusProperties.CCS_MACHINE_ID);
    QuarkusSystemPropertyUtil.refresh();
    assertNull(QuarkusProperties.getCcsMachineId());
  }

  @Test
  final void testSystemPropertyString() {
    QuarkusSystemPropertyUtil.refresh();
    QuarkusSystemPropertyUtil.set(KEY_TEST, KEY_VALUE);
    assertTrue(QuarkusSystemPropertyUtil.contains(KEY_TEST));
    assertEquals(KEY_VALUE, QuarkusSystemPropertyUtil.get(KEY_TEST));
    assertEquals(KEY_VALUE, QuarkusSystemPropertyUtil.get(KEY_TEST, OTHER));
    assertEquals(KEY_VALUE, QuarkusSystemPropertyUtil.getAndSet(KEY_TEST, OTHER));
    assertEquals(OTHER, QuarkusSystemPropertyUtil.getAndSet(KEY_TEST + '2', OTHER));
    assertEquals(OTHER, QuarkusSystemPropertyUtil.set(KEY_TEST + '2', OTHER));
  }

  @Test
  final void testSystemPropertyBoolean() {
    QuarkusSystemPropertyUtil.set(KEY_BTEST, KEY_BVALUE);
    assertTrue(QuarkusSystemPropertyUtil.contains(KEY_BTEST));
    assertEquals(Boolean.toString(KEY_BVALUE), QuarkusSystemPropertyUtil.get(KEY_BTEST));
    assertEquals(KEY_BVALUE, QuarkusSystemPropertyUtil.get(KEY_BTEST, false));
    assertEquals(KEY_BVALUE, QuarkusSystemPropertyUtil.getAndSet(KEY_BTEST, false));
    assertFalse(QuarkusSystemPropertyUtil.getAndSet(KEY_BTEST + '2', false));
    assertFalse(QuarkusSystemPropertyUtil.set(KEY_BTEST + '2', false));
    assertFalse(QuarkusSystemPropertyUtil.get(KEY_BTEST + '3', false));
    assertNull(QuarkusSystemPropertyUtil.set(KEY_BTEST + '3', "true"));
    assertTrue(QuarkusSystemPropertyUtil.get(KEY_BTEST + '3', false));
    assertEquals("true", QuarkusSystemPropertyUtil.set(KEY_BTEST + '3', "yes"));
    assertTrue(QuarkusSystemPropertyUtil.get(KEY_BTEST + '3', false));
    assertEquals("yes", QuarkusSystemPropertyUtil.set(KEY_BTEST + '3', "1"));
    assertTrue(QuarkusSystemPropertyUtil.get(KEY_BTEST + '3', false));
    assertEquals("1", QuarkusSystemPropertyUtil.set(KEY_BTEST + '3', "yes2"));
    assertFalse(QuarkusSystemPropertyUtil.get(KEY_BTEST + '3', false));
    assertEquals("yes2", QuarkusSystemPropertyUtil.set(KEY_BTEST + '3', ""));
    assertTrue(QuarkusSystemPropertyUtil.get(KEY_BTEST + '3', false));
  }

  @Test
  final void testSystemPropertyInt() {
    QuarkusSystemPropertyUtil.set(KEY_ITEST, KEY_IVALUE);
    assertTrue(QuarkusSystemPropertyUtil.contains(KEY_ITEST));
    assertEquals(Integer.toString(KEY_IVALUE), QuarkusSystemPropertyUtil.get(KEY_ITEST));
    assertEquals(KEY_IVALUE, QuarkusSystemPropertyUtil.get(KEY_ITEST, 4));
    assertEquals(KEY_IVALUE, QuarkusSystemPropertyUtil.getAndSet(KEY_ITEST, 4));
    assertEquals(4, QuarkusSystemPropertyUtil.getAndSet(KEY_ITEST + '2', 4));
    assertEquals(4, QuarkusSystemPropertyUtil.set(KEY_ITEST + '2', 4));
    assertEquals(5, QuarkusSystemPropertyUtil.get(KEY_ITEST + '3', 5));
    assertNull(QuarkusSystemPropertyUtil.set(KEY_ITEST + '3', "yes2"));
    assertEquals(6, QuarkusSystemPropertyUtil.get(KEY_ITEST + '3', 6));
  }

  @Test
  final void testSystemPropertyLong() {
    QuarkusSystemPropertyUtil.set(KEY_LTEST, KEY_LVALUE);
    assertTrue(QuarkusSystemPropertyUtil.contains(KEY_LTEST));
    assertEquals(Long.toString(KEY_LVALUE), QuarkusSystemPropertyUtil.get(KEY_LTEST));
    assertEquals(KEY_LVALUE, QuarkusSystemPropertyUtil.get(KEY_LTEST, 3L));
    assertEquals(KEY_LVALUE, QuarkusSystemPropertyUtil.getAndSet(KEY_LTEST, 3L));
    assertEquals(3L, QuarkusSystemPropertyUtil.getAndSet(KEY_LTEST + '2', 3L));
    assertEquals(3L, QuarkusSystemPropertyUtil.set(KEY_LTEST + '2', 3L));
    assertEquals(4L, QuarkusSystemPropertyUtil.get(KEY_LTEST + '3', 4L));
    assertNull(QuarkusSystemPropertyUtil.set(KEY_LTEST + '3', "yes2"));
    assertEquals(5L, QuarkusSystemPropertyUtil.get(KEY_LTEST + '3', 5L));
  }

  @Test
  final void testConfigProperty() {
    assertNull(QuarkusSystemPropertyUtil.getBooleanConfig("test.key5"));
    assertNull(QuarkusSystemPropertyUtil.getStringConfig("test.key5"));
    assertNull(QuarkusSystemPropertyUtil.getLongConfig("test.key5"));
    assertNull(QuarkusSystemPropertyUtil.getBooleanConfig("test.key5"));

    assertEquals("test2", QuarkusSystemPropertyUtil.getStringConfig("test.key1"));
    assertEquals("test2", QuarkusSystemPropertyUtil.getStringConfig("test.key1", "default"));
    assertEquals("test2", QuarkusSystemPropertyUtil.getStringConfig("test.key1", null, "default"));
    assertEquals("test3", QuarkusSystemPropertyUtil.getStringConfig("test.key1", "test3", "default"));
    assertEquals("default", QuarkusSystemPropertyUtil.getStringConfig("test.key5", "default"));

    assertEquals(1234, QuarkusSystemPropertyUtil.getLongConfig("test.key2"));
    assertEquals(1234, QuarkusSystemPropertyUtil.getLongConfig("test.key2", 1000));
    assertEquals(1234, QuarkusSystemPropertyUtil.getLongConfig("test.key2", null, 1000));
    assertEquals(10, QuarkusSystemPropertyUtil.getLongConfig("test.key2", 10L, 1000));
    assertEquals(1000, QuarkusSystemPropertyUtil.getLongConfig("test.key5", 1000));

    assertEquals(1234, QuarkusSystemPropertyUtil.getIntegerConfig("test.key2"));
    assertEquals(1234, QuarkusSystemPropertyUtil.getIntegerConfig("test.key2", 1000));
    assertEquals(1234, QuarkusSystemPropertyUtil.getIntegerConfig("test.key2", null, 1000));
    assertEquals(10, QuarkusSystemPropertyUtil.getIntegerConfig("test.key2", 10, 1000));
    assertEquals(1000, QuarkusSystemPropertyUtil.getIntegerConfig("test.key5", 1000));

    assertEquals(true, QuarkusSystemPropertyUtil.getBooleanConfig("test.key3"));
    assertTrue(QuarkusSystemPropertyUtil.getBooleanConfig("test.key3", false));
    assertTrue(QuarkusSystemPropertyUtil.getBooleanConfig("test.key3", null, false));
    assertFalse(QuarkusSystemPropertyUtil.getBooleanConfig("test.key3", false, false));
    assertFalse(QuarkusSystemPropertyUtil.getBooleanConfig("test.key5", false));

    assertEquals(true, QuarkusSystemPropertyUtil.getBooleanConfig("test.key4"));
    assertTrue(QuarkusSystemPropertyUtil.getBooleanConfig("test.key4", false));
    assertTrue(QuarkusSystemPropertyUtil.getBooleanConfig("test.key4", null, false));
    assertFalse(QuarkusSystemPropertyUtil.getBooleanConfig("test.key4", false, false));
    assertFalse(QuarkusSystemPropertyUtil.getBooleanConfig("test.key5", false));
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
    QuarkusSystemPropertyUtil.debug(out);
    assertTrue(bool.get());
  }

  @Test
  final void testSystemPropertyOs() {
    QuarkusSystemPropertyUtil.get(OS_NAME);
    final var platform = QuarkusSystemPropertyUtil.getOS();
    switch (platform) {
      case MAC: {
        assertTrue(QuarkusSystemPropertyUtil.isMac());
        assertFalse(QuarkusSystemPropertyUtil.isWindows());
        assertFalse(QuarkusSystemPropertyUtil.isUnix());
        assertFalse(QuarkusSystemPropertyUtil.isSolaris());
        break;
      }
      case SOLARIS: {
        assertFalse(QuarkusSystemPropertyUtil.isMac());
        assertFalse(QuarkusSystemPropertyUtil.isWindows());
        assertFalse(QuarkusSystemPropertyUtil.isUnix());
        assertTrue(QuarkusSystemPropertyUtil.isSolaris());
        break;
      }
      case UNIX: {
        assertFalse(QuarkusSystemPropertyUtil.isMac());
        assertFalse(QuarkusSystemPropertyUtil.isWindows());
        assertTrue(QuarkusSystemPropertyUtil.isUnix());
        assertFalse(QuarkusSystemPropertyUtil.isSolaris());
        break;
      }
      case UNSUPPORTED: {
        assertFalse(QuarkusSystemPropertyUtil.isMac());
        assertFalse(QuarkusSystemPropertyUtil.isWindows());
        assertFalse(QuarkusSystemPropertyUtil.isUnix());
        assertFalse(QuarkusSystemPropertyUtil.isSolaris());
        break;
      }
      case WINDOWS: {
        assertFalse(QuarkusSystemPropertyUtil.isMac());
        assertTrue(QuarkusSystemPropertyUtil.isWindows());
        assertFalse(QuarkusSystemPropertyUtil.isUnix());
        assertFalse(QuarkusSystemPropertyUtil.isSolaris());
        break;
      }
      default: {
      }
    }
  }

  @Test
  final void testSystemPropertyError() {
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> QuarkusSystemPropertyUtil.contains(null));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> QuarkusSystemPropertyUtil.get(null));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> QuarkusSystemPropertyUtil.get(null, KEY_IVALUE));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> QuarkusSystemPropertyUtil.get(null, KEY_BVALUE));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> QuarkusSystemPropertyUtil.get(null, KEY_LVALUE));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> QuarkusSystemPropertyUtil.getAndSet(null, KEY_VALUE));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> QuarkusSystemPropertyUtil.getAndSet(null, KEY_BVALUE));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> QuarkusSystemPropertyUtil.getAndSet(null, KEY_IVALUE));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> QuarkusSystemPropertyUtil.getAndSet(null, KEY_LVALUE));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> QuarkusSystemPropertyUtil.set(null, KEY_VALUE));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> QuarkusSystemPropertyUtil.set(null, KEY_BVALUE));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> QuarkusSystemPropertyUtil.set(null, KEY_IVALUE));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> QuarkusSystemPropertyUtil.set(null, KEY_LVALUE));
  }
}
