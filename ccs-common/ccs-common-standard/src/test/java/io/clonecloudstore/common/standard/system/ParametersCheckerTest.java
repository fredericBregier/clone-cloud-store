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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Parameters Checker Test
 */
@QuarkusTest
class ParametersCheckerTest {
  private final List tocheck = SingletonUtils.singletonList();

  @Test
  final void testCheckParamaterObjectArray() {
    // String
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> ParametersChecker.checkParameter("test message", (Object[]) null));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> ParametersChecker.checkParameter("test message", null, "notnull"));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> ParametersChecker.checkParameter("test message", "notnull", null));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> ParametersChecker.checkParameter("test message", "", "notnull"));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> ParametersChecker.checkParameter("test message", "notnull", ""));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> ParametersChecker.checkParameter("test message", "notnull", " "));
    try {
      ParametersChecker.checkParameter("test message", "notNull", "notnull");
      ParametersChecker.checkParameter("test message", "notnull");
    } catch (final CcsInvalidArgumentRuntimeException e) {
      fail("SHOULD_NOT_RAISED_ILLEGAL_ARGUMENT_EXCEPTION");
    }
    // Object
    try {
      ParametersChecker.checkParameter("test message", tocheck, tocheck);
      ParametersChecker.checkParameter("test message", tocheck, "notnull");
    } catch (final CcsInvalidArgumentRuntimeException e) {
      fail("SHOULD_NOT_RAISED_ILLEGAL_ARGUMENT_EXCEPTION");
    }
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> ParametersChecker.checkParameter("test message", null, " "));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> ParametersChecker.checkParameter("test message", (Object[]) null));
    final List<String> list = new ArrayList<>();
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> ParametersChecker.checkParameter("test message", null, list));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> ParametersChecker.checkParameter("test message", list, null));
    try {
      ParametersChecker.checkParameter("test message", list, list);
      ParametersChecker.checkParameter("test message", list);
    } catch (final CcsInvalidArgumentRuntimeException e) {
      fail("SHOULD_NOT_RAISED_ILLEGAL_ARGUMENT_EXCEPTION");
    }
  }

  @Test
  final void testIsNotEmptyObjectArray() {
    try {
      assertFalse(ParametersChecker.isNotEmpty((Object[]) null));
      assertFalse(ParametersChecker.isNotEmpty("test message", null, "notnull"));
      assertFalse(ParametersChecker.isNotEmpty("test message", "notnull", null));
      assertFalse(ParametersChecker.isNotEmpty("test message", "", "notnull"));
      assertFalse(ParametersChecker.isNotEmpty("test message", "notnull", ""));
      assertFalse(ParametersChecker.isNotEmpty("test message", "notnull", " "));
      assertTrue(ParametersChecker.isNotEmpty("test message", "notNull", "notnull"));
      assertTrue(ParametersChecker.isNotEmpty("test message", "notnull"));
    } catch (final CcsInvalidArgumentRuntimeException e) {
      fail("SHOULD_NOT_RAISED_ILLEGAL_ARGUMENT_EXCEPTION");
    }
    // Object
    try {
      assertFalse(ParametersChecker.isNotEmpty("test message", tocheck, " "));
      assertTrue(ParametersChecker.isNotEmpty("test message", tocheck, "notnull"));
      assertTrue(ParametersChecker.isNotEmpty("test message", tocheck));
    } catch (final CcsInvalidArgumentRuntimeException e) {
      fail("SHOULD_NOT_RAISED_ILLEGAL_ARGUMENT_EXCEPTION");
    }
  }

  @Test
  final void testIsEmptyObjectArray() {
    try {
      assertTrue(ParametersChecker.isEmpty((Object[]) null));
      assertTrue(ParametersChecker.isEmpty("test message", null, "notnull"));
      assertTrue(ParametersChecker.isEmpty("test message", "notnull", null));
      assertTrue(ParametersChecker.isEmpty("test message", "", "notnull"));
      assertTrue(ParametersChecker.isEmpty("test message", "notnull", ""));
      assertTrue(ParametersChecker.isEmpty("test message", "notnull", " "));
      assertTrue(ParametersChecker.isEmpty("test message", tocheck, " "));
      assertFalse(ParametersChecker.isEmpty("test message", "notNull", "notnull"));
      assertFalse(ParametersChecker.isEmpty("test message", "notnull"));
      assertFalse(ParametersChecker.isEmpty("test message", tocheck));
    } catch (final CcsInvalidArgumentRuntimeException e) {
      fail("SHOULD_NOT_RAISED_ILLEGAL_ARGUMENT_EXCEPTION");
    }
  }

  @Test
  final void testCheckParamaterNullOnlyObjectArray() {
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> ParametersChecker.checkParameterNullOnly("test message", (Object[]) null));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> ParametersChecker.checkParameterNullOnly("test message", null, "notnull"));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> ParametersChecker.checkParameterNullOnly("test message", "notnull", null));
    try {
      ParametersChecker.checkParameterNullOnly("test message", "", "notnull");
    } catch (final CcsInvalidArgumentRuntimeException e) {
      fail("SHOULD_NOT_RAISED_ILLEGAL_ARGUMENT_EXCEPTION");
    }
    try {
      ParametersChecker.checkParameterNullOnly("test message", "notnull", "");
    } catch (final CcsInvalidArgumentRuntimeException e) {
      fail("SHOULD_NOT_RAISED_ILLEGAL_ARGUMENT_EXCEPTION");
    }
    try {
      ParametersChecker.checkParameterNullOnly("test message", "notNull", "notnull");
      ParametersChecker.checkParameterNullOnly("test message", "notnull");
    } catch (final CcsInvalidArgumentRuntimeException e) {
      fail("SHOULD_NOT_RAISED_ILLEGAL_ARGUMENT_EXCEPTION");
    }
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> ParametersChecker.checkParameterNullOnly("test message", tocheck, null));
    try {
      ParametersChecker.checkParameterNullOnly("test message", tocheck, "notnull");
      ParametersChecker.checkParameterNullOnly("test message", tocheck);
    } catch (final CcsInvalidArgumentRuntimeException e) {
      fail("SHOULD_NOT_RAISED_ILLEGAL_ARGUMENT_EXCEPTION");
    }
  }

  @Test
  final void testCheckValue() {
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> ParametersChecker.checkValue("test", 1, 2));
    ParametersChecker.checkValue("test", 1, 1);
    ParametersChecker.checkValue("test", 1, 0);
  }

  @Test
  final void testSanity() {
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> ParametersChecker.checkSanityString("test\b"));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> ParametersChecker.checkSanityString("test\n"));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> ParametersChecker.checkSanityString("test\b", "test"));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> ParametersChecker.checkSanityString("test", "test\b"));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> ParametersChecker.checkSanityString("test" + "<![CDATA[", "test"));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> ParametersChecker.checkSanityString("<![CDATA[test", "test"));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> ParametersChecker.checkSanityString("test" + "<![CDATA[test", "test"));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> ParametersChecker.checkSanityUri("wrong uri"));

    ParametersChecker.checkSanityObjectName("validName1234/with-various.com");
    ParametersChecker.checkSanityBucketName("validname1234");
    ParametersChecker.checkSanityBucketName("clientid-validname1234");
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> ParametersChecker.checkSanityString("test" + ";test", "test"));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> ParametersChecker.checkSanityBucketName("test@invalid"));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> ParametersChecker.checkSanityBucketName(
        "testinvalidaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> ParametersChecker.getSanitizedBucketName(null));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> ParametersChecker.getSanitizedBucketName("  "));
    assertThrows(CcsInvalidArgumentRuntimeException.class,
        () -> ParametersChecker.checkSanityObjectName("test@invalid"));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> ParametersChecker.checkSanityObjectName(
        "testinvalidaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
            "testinvalidaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
            "testinvalidaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
            "testinvalidaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
            "testinvalidaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
            "testinvalidaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
            "testinvalidaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
            "testinvalidaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
            "testinvalidaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
            "testinvalidaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
            "testinvalidaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> ParametersChecker.getSanitizedObjectName(null));
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> ParametersChecker.getSanitizedObjectName("  "));

    ParametersChecker.checkSanityString("test");
    ParametersChecker.checkSanityString("test", "test", "", null);

    Map<String, String> map = new HashMap<>();
    ParametersChecker.checkSanityMapKey(null);
    ParametersChecker.checkSanityMap(map);
    map.put("validValid0_Valid", "any-thing");
    ParametersChecker.checkSanityMap(map);
    map.put("0_invalid_key", "any-thing");
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> ParametersChecker.checkSanityMap(map));
    map.remove("0_invalid");
    map.put("invalid-key", "any-thing");
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> ParametersChecker.checkSanityMap(map));
    map.remove("invalid-key");
    map.put("invalid key", "any-thing");
    assertThrows(CcsInvalidArgumentRuntimeException.class, () -> ParametersChecker.checkSanityMap(map));
  }

  @Test
  final void testHasNotEmpty() {
    try {
      assertTrue(ParametersChecker.hasNotEmpty("test\n"));
      assertTrue(ParametersChecker.hasNotEmpty(tocheck, "test\n"));
      assertTrue(ParametersChecker.hasNotEmpty(tocheck, ""));
      assertTrue(ParametersChecker.hasNotEmpty("test\n", null));
      assertFalse(ParametersChecker.hasNotEmpty("", " "));
      assertFalse(ParametersChecker.hasNotEmpty(null, " "));
      assertFalse(ParametersChecker.hasNotEmpty((Object[]) null));
      assertFalse(ParametersChecker.hasNotEmpty((Object) null));
      assertFalse(ParametersChecker.hasNotEmpty("", ""));
    } catch (final CcsInvalidArgumentRuntimeException e) {
      fail(e);
    }
  }

  @Test
  void emptyObjectName() {
    final var res = ParametersChecker.getSanitizedName("  ");
    assertNull(res);
  }

  @Test
  void backslashObjectName() {
    final var res = ParametersChecker.getSanitizedName("\\test\\name");
    assertEquals("test/name", res);
  }

  @Test
  void missingPrefixObjectName() {
    final var res = ParametersChecker.getSanitizedName("test/name");
    assertEquals("test/name", res);
  }

  @Test
  void slashPrefixObjectName() {
    final var res = ParametersChecker.getSanitizedName("/test/name");
    assertEquals("test/name", res);
  }

  @Test
  void accentAndNotAscii() {
    final var res = ParametersChecker.getSanitizedName("/test/Name with words ");
    assertEquals("test/Name_with_words", res);
    assertEquals("test_other_Final_word", ParametersChecker.getSanitizedName("__test__other   Final word_"));
    assertEquals("-test_other-_Final_word-", ParametersChecker.getSanitizedName("  ---test__other---   Final word---"));
    assertEquals("aaaeiiiiggnnsssuuy", ParametersChecker.getSanitizedName("āăąēîïĩíĝġńñšŝśûůŷ"));
    assertEquals("aaaeiiiiggnnsssuuy".toUpperCase(),
        ParametersChecker.getSanitizedName("āăąēîïĩíĝġńñšŝśûůŷ").toUpperCase());
    // Not all
    assertEquals("l", ParametersChecker.getSanitizedName("łđħœ"));
    assertEquals("aaaeiiiiggnnsssuuy/aaaeiiiiggnnsssuuy_aaaeiiiiggnnsssuuy",
        ParametersChecker.getSanitizedName("//āăąēîïĩíĝġńñšŝśûůŷ\\āăąēîïĩíĝġńñšŝśûůŷ āăąēîïĩíĝġńñšŝśûůŷ  "));
  }

  @Test
  void encodedName() {
    final var res = ParametersChecker.getSanitizedName("%2Ftest%2Fsecond");
    assertEquals("test/second", res);
  }
}
