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

import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.common.standard.system.SysErrLogger.ANSI_BLACK;
import static io.clonecloudstore.common.standard.system.SysErrLogger.ANSI_BLUE;
import static io.clonecloudstore.common.standard.system.SysErrLogger.ANSI_CYAN;
import static io.clonecloudstore.common.standard.system.SysErrLogger.ANSI_GREEN;
import static io.clonecloudstore.common.standard.system.SysErrLogger.ANSI_PURPLE;
import static io.clonecloudstore.common.standard.system.SysErrLogger.ANSI_RED;
import static io.clonecloudstore.common.standard.system.SysErrLogger.ANSI_RESET;
import static io.clonecloudstore.common.standard.system.SysErrLogger.ANSI_WHITE;
import static io.clonecloudstore.common.standard.system.SysErrLogger.ANSI_YELLOW;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class SysErrLoggerTest {
  private static final String NOT_EMPTY = "Not empty";
  private static final StringBuilder buf = new StringBuilder();
  private static PrintStream err;
  private static PrintStream out;

  @BeforeAll
  static void setUpBeforeClass() {
    err = System.err; // NOSONAR since Logger test
    System.setErr(new PrintStream(new OutputStream() {
      @Override
      public void write(final int b) {
        buf.append((char) b);
      }
    }, true, StandardCharsets.UTF_8));
    out = System.out; // NOSONAR since Logger test
    System.setOut(new PrintStream(new OutputStream() {
      @Override
      public void write(final int b) {
        buf.append((char) b);
      }
    }, true, StandardCharsets.UTF_8));
  }

  @AfterAll
  static void tearDownAfterClass() {
    System.setErr(err);
    System.setOut(out);
  }

  @Test
  void testSyserr() {
    buf.setLength(0);
    SysErrLogger.FAKE_LOGGER.ignoreLog(new Exception("Fake exception"));
    assertEquals(0, buf.length());
    SysErrLogger.FAKE_LOGGER.syserr(NOT_EMPTY);
    assertTrue(buf.length() > 0);
    buf.setLength(0);
    SysErrLogger.FAKE_LOGGER.syserr();
    assertTrue(buf.length() > 0);
    buf.setLength(0);
    SysErrLogger.FAKE_LOGGER.syserr(NOT_EMPTY, new Exception("Fake exception"));
    assertTrue(buf.length() > NOT_EMPTY.length() + 5);
    buf.setLength(0);
    SysErrLogger.FAKE_LOGGER.syserr(new Exception("Fake exception"));
    assertTrue(buf.length() > 5);
    buf.setLength(0);
  }

  @Test
  void testSysout() {
    buf.setLength(0);
    SysErrLogger.FAKE_LOGGER.ignoreLog(new Exception("Fake exception"));
    assertEquals(0, buf.length());
    SysErrLogger.FAKE_LOGGER.sysout(NOT_EMPTY);
    assertTrue(buf.length() > 0);
    buf.setLength(0);
    SysErrLogger.FAKE_LOGGER.sysout();
    assertTrue(buf.length() > 0);
    buf.setLength(0);
  }

  @Test
  void testColor() {
    buf.setLength(0);
    SysErrLogger.FAKE_LOGGER.syserr(
        SysErrLogger.cyan("cyan") + " " + SysErrLogger.black("black") + " " + SysErrLogger.blue("blue") + " " +
            SysErrLogger.green("green") + " " + SysErrLogger.purple("purple") + " " + SysErrLogger.red("red") + " " +
            SysErrLogger.white("white") + " " + SysErrLogger.yellow("yellow"));
    assertTrue(buf.length() > 0);
    assertTrue(buf.indexOf(ANSI_CYAN) >= 0);
    assertTrue(buf.indexOf(ANSI_BLACK) >= 0);
    assertTrue(buf.indexOf(ANSI_BLUE) >= 0);
    assertTrue(buf.indexOf(ANSI_GREEN) >= 0);
    assertTrue(buf.indexOf(ANSI_PURPLE) >= 0);
    assertTrue(buf.indexOf(ANSI_RED) >= 0);
    assertTrue(buf.indexOf(ANSI_WHITE) >= 0);
    assertTrue(buf.indexOf(ANSI_YELLOW) >= 0);
    assertTrue(buf.indexOf(ANSI_RED) >= 0);
    assertTrue(buf.indexOf(ANSI_RED) >= 0);
    assertTrue(buf.indexOf(ANSI_RESET) >= 0);
    buf.setLength(0);
  }
}
