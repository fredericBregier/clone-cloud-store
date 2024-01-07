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

import java.util.Random;

/**
 * Efficient Random generator using ThreadLocalRandom
 */
public final class RandomUtil {
  private RandomUtil() {
    // empty
  }

  /**
   * @param length the length of rray
   * @return a byte array with random values
   */
  public static byte[] getRandom(final int length) {
    if (length <= 0) {
      return SingletonUtils.getSingletonByteArray();
    }
    final var random = SystemRandomSecure.getSecureRandomSingleton();
    final var result = new byte[length];
    for (var i = 0; i < result.length; i++) {
      result[i] = (byte) (random.nextInt(95) + 32);
    }
    return result;
  }

  public static Random getRandom() {
    return SystemRandomSecure.getSecureRandomSingleton();
  }
}
