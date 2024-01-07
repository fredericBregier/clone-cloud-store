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

package io.clonecloudstore.common.standard.inputstream;

import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;

/**
 * Enum on standard Digest algorithms
 */
public enum DigestAlgo {
  MD5("MD5", 16),
  MD2("MD2", 16),
  SHA1("SHA-1", 20),
  SHA256("SHA-256", 32),
  SHA384("SHA-384", 48),
  SHA512("SHA-512", 64),
  SHA3_256("SHA3-256", 64);

  public final String algoName;
  public final int byteSize;

  DigestAlgo(final String algoName, final int byteSize) {
    this.algoName = algoName;
    this.byteSize = byteSize;
  }

  public static DigestAlgo getFromName(final String name) {
    try {
      return valueOf(name);
    } catch (final IllegalArgumentException ignore) {// NOSONAR intentional
      // ignore
    }
    if (MD5.algoName.equalsIgnoreCase(name)) {
      return MD5;
    } else if (MD2.algoName.equalsIgnoreCase(name)) {
      return MD2;
    } else if (SHA1.algoName.equalsIgnoreCase(name)) {
      return SHA1;
    } else if (SHA256.algoName.equalsIgnoreCase(name)) {
      return SHA256;
    } else if (SHA384.algoName.equalsIgnoreCase(name)) {
      return SHA384;
    } else if (SHA512.algoName.equalsIgnoreCase(name)) {
      return SHA512;
    } else if (SHA3_256.algoName.equalsIgnoreCase(name)) {
      return SHA3_256;
    } else {
      throw new CcsInvalidArgumentRuntimeException("Digest Algo not found: " + name);
    }
  }

  /**
   * @return the length in bytes of one Digest
   */
  public final int getByteSize() {
    return byteSize;
  }

  /**
   * @return the length in Hex form of one Digest
   */
  public final int getHexSize() {
    return byteSize * 2;
  }
}
