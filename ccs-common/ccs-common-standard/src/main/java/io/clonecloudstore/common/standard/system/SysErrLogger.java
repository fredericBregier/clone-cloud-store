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

/**
 * Utility class to be used only in classes where standard Logger is not allowed
 */
public final class SysErrLogger {
  /**
   * FAKE LOGGER used where no LOG could be done
   */
  public static final SysErrLogger FAKE_LOGGER = new SysErrLogger();
  // ANSI escape code
  public static final String ANSI_RESET = "\u001B[0m";
  public static final String ANSI_BLACK = "\u001B[30m";
  public static final String ANSI_RED = "\u001B[31m";
  public static final String ANSI_GREEN = "\u001B[32m";
  public static final String ANSI_YELLOW = "\u001B[33m";
  public static final String ANSI_BLUE = "\u001B[34m";
  public static final String ANSI_PURPLE = "\u001B[35m";
  public static final String ANSI_CYAN = "\u001B[36m";
  public static final String ANSI_WHITE = "\u001B[37m";

  private SysErrLogger() {
    // Empty
  }

  public static String red(final String msg) {
    return ANSI_RED + msg + ANSI_RESET;
  }

  public static String green(final String msg) {
    return ANSI_GREEN + msg + ANSI_RESET;
  }

  public static String yellow(final String msg) {
    return ANSI_YELLOW + msg + ANSI_RESET;
  }

  public static String blue(final String msg) {
    return ANSI_BLUE + msg + ANSI_RESET;
  }

  public static String purple(final String msg) {
    return ANSI_PURPLE + msg + ANSI_RESET;
  }

  public static String cyan(final String msg) {
    return ANSI_CYAN + msg + ANSI_RESET;
  }

  public static String white(final String msg) {
    return ANSI_WHITE + msg + ANSI_RESET;
  }

  public static String black(final String msg) {
    return ANSI_BLACK + msg + ANSI_RESET;
  }

  /**
   * Utility method to log nothing
   *
   * @param throwable to log ignore
   */
  public void ignoreLog(final Throwable throwable) {// NOSONAR intentional
    // Nothing to do
  }

  /**
   * Utility method to log through System.out
   */
  public void sysout() {
    System.out.println(); // NOSONAR intentional
  }

  /**
   * Utility method to log through System.out
   *
   * @param message to write for no error log
   */
  public void sysout(final Object message) {
    System.out.println(message); // NOSONAR intentional
  }

  /**
   * Utility method to log through System.err
   *
   * @param message to write for error
   */
  public void syserr(final Object message) {
    System.err.println("ERROR " + message); // NOSONAR intentional
  }

  /**
   * Utility method to log through System.err the current Stacktrace
   */
  public void syserr() {
    new RuntimeException("ERROR Stacktrace").printStackTrace(); // NOSONAR intentional
  }

  /**
   * Utility method to log through System.err the current Stacktrace
   *
   * @param message to write for error
   * @param e       throw to write as error
   */
  public void syserr(final String message, final Throwable e) {
    System.err.print("ERROR " + message + ": "); // NOSONAR intentional
    e.printStackTrace(); // NOSONAR intentional
  }

  /**
   * Utility method to log through System.err the current Stacktrace
   *
   * @param e throw to write as error
   */
  public void syserr(final Throwable e) {
    System.err.print("ERROR: "); // NOSONAR intentional
    e.printStackTrace(); // NOSONAR intentional
  }
}
