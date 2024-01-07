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

import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.Security;

/**
 * Improve Random generation
 */
public final class SystemRandomSecure {
  private static final String OS_NAME = "os.name";
  private static final String WINDOWS = "Windows";
  private static final String SUN_MSCAPI = "SunMSCAPI";
  private static final String JAVA_SECURITY_EGD = "java.security.egd";
  private static final String FILE_DEV_URANDOM = "file:/dev/./urandom";
  private static final String SUN = "SUN";
  private static final String SECURE_RANDOM = "SecureRandom";
  private static final String NATIVE_PRNGNON_BLOCKING = "NativePRNGNonBlocking";
  private static final String WINDOWS_PRNG = "Windows-PRNG";
  private static volatile boolean initialized;
  private static boolean specialSecureRandom;
  private static SecureRandom secureRandom = null;

  static {
    initializedRandomContext();
  }

  private SystemRandomSecure() {
    // Nothing
  }

  static void initializedRandomContext() {
    try {
      if (!initialized) {
        registerRandomSecure();
        initialized = true;
      }
    } catch (final RuntimeException throwable) {// NOSONAR intentional
      SysErrLogger.FAKE_LOGGER.syserr("Error occurs at startup: " +// NOSONAR intentional
          throwable.getMessage(), throwable);
    }
    secureRandom = getSecureRandom();
  }

  /**
   * To fix issue on SecureRandom using bad algorithm
   * <br/>
   * Called at second place
   */
  private static void registerRandomSecure() {
    if (System.getProperty(OS_NAME).contains(WINDOWS)) {
      final var provider = Security.getProvider(SUN_MSCAPI);
      if (provider != null) {
        Security.removeProvider(provider.getName());
        Security.insertProviderAt(provider, 1);
        specialSecureRandom = true;
      }
    } else {
      System.setProperty(JAVA_SECURITY_EGD, FILE_DEV_URANDOM);
      final var provider = Security.getProvider(SUN);
      final var type = SECURE_RANDOM;
      final var alg = NATIVE_PRNGNON_BLOCKING;
      if (provider != null) {
        final var name = String.format("%s.%s", type, alg);
        final var service = provider.getService(type, alg);
        if (service != null) {
          System.setProperty(name, service.getClassName());
          Security.insertProviderAt(
              new Provider(name, provider.getVersionStr(), "Quick fix for SecureRandom using urandom") {
              }, 1);
          specialSecureRandom = true;
        }
      }
    }
  }

  public static SecureRandom getSecureRandom() {
    try {
      if (specialSecureRandom && System.getProperty(OS_NAME).contains(WINDOWS)) {
        return SecureRandom.getInstance(WINDOWS_PRNG, SUN_MSCAPI);
      } else if (specialSecureRandom) {
        return SecureRandom.getInstance(NATIVE_PRNGNON_BLOCKING, SUN);
      }
    } catch (final NoSuchAlgorithmException | NoSuchProviderException ignore) {
      // Ignore
    }
    return new SecureRandom();
  }

  /**
   * @return a singleton instance instead of a new one
   */
  public static SecureRandom getSecureRandomSingleton() {
    return secureRandom;
  }

  /**
   * @param length the length of rray
   * @return a byte array with random values
   */
  public static byte[] getRandom(final int length) {
    if (length <= 0) {
      return SingletonUtils.getSingletonByteArray();
    }
    final var result = new byte[length];
    for (var i = 0; i < result.length; i++) {
      result[i] = (byte) (secureRandom.nextInt(95) + 32);
    }
    return result;
  }
}
