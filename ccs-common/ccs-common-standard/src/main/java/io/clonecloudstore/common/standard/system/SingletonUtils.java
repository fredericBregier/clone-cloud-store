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

import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Singleton utility class
 */
public final class SingletonUtils {
  private static final byte[] SINGLETON_BYTE_ARRAY = {};
  private static final InputStream SINGLETON_INPUTSTREAM = InputStream.nullInputStream();

  private SingletonUtils() {
    // empty
  }


  /**
   * Immutable empty byte array
   *
   * @return a Byte Array Singleton
   */
  public static byte[] getSingletonByteArray() {
    return SINGLETON_BYTE_ARRAY;
  }

  /**
   * Immutable empty List
   *
   * @return an immutable empty List
   */
  public static <E> List<E> singletonList() {
    return Collections.emptyList();
  }

  /**
   * Immutable empty Set
   *
   * @return an immutable empty Set
   */
  public static <E> Set<E> singletonSet() {
    return Collections.emptySet();
  }

  /**
   * Immutable empty Map
   *
   * @return an immutable empty Map
   */
  public static <E, V> Map<E, V> singletonMap() {
    return Collections.emptyMap();
  }

  /**
   * Immutable empty InputStream
   *
   * @return an immutable empty InputStream
   */
  public static InputStream singletonInputStream() {
    return SINGLETON_INPUTSTREAM;
  }

}
