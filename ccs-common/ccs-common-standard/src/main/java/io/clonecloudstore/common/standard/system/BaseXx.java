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

import java.util.Arrays;
import java.util.Base64;

import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import org.apache.commons.codec.binary.Base16;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.binary.StringUtils;

/**
 * Base16, Base32 and Base64 codecs
 */
public final class BaseXx {
  public static final int HASH_LENGTH = 128;
  private static final String ARGUMENT_NULL_NOT_ALLOWED = "argument null not allowed";
  private static final Base64.Encoder BASE64 = Base64.getEncoder().withoutPadding();
  private static final Base64.Decoder BASE64_DEC = Base64.getDecoder();
  private static final Base64.Encoder BASE64STANDARD = Base64.getEncoder();
  private static final Base64.Encoder BASE64URL = Base64.getUrlEncoder().withoutPadding();
  private static final Base64.Decoder BASE64URL_DEC = Base64.getUrlDecoder();
  private static final Base32 BASE32 = new Base32(true);
  private static final Base16 BASE16 = new Base16(true);
  private static final Boolean NOT_NULL = Boolean.TRUE;

  private BaseXx() {
    // empty
  }

  /**
   * @param bytes to transform
   * @return the Base 16 representation Without Padding representation
   * @throws CcsInvalidArgumentRuntimeException if argument is not compatible
   */
  public static String getBase16(final byte[] bytes) {
    ParametersChecker.checkParameter(ARGUMENT_NULL_NOT_ALLOWED, bytes, NOT_NULL);
    return BASE16.encodeToString(bytes).toLowerCase().replace("=", "");
  }

  /**
   * @param bytes  to transform
   * @param offset offset to start from
   * @param size   size to use from offset
   * @return the Base 16 representation Without Padding representation
   * @throws CcsInvalidArgumentRuntimeException if argument is not compatible
   */
  public static String getBase16(final byte[] bytes, final int offset, final int size) {
    ParametersChecker.checkParameter(ARGUMENT_NULL_NOT_ALLOWED, bytes, NOT_NULL);
    return StringUtils.newStringUtf8(BASE16.encode(bytes, offset, size)).toLowerCase().replace("=", "");
  }

  /**
   * @param bytes to transform
   * @return the Base 32 representation Without Padding representation
   * @throws CcsInvalidArgumentRuntimeException if argument is not compatible
   */
  public static String getBase32(final byte[] bytes) {
    ParametersChecker.checkParameter(ARGUMENT_NULL_NOT_ALLOWED, bytes, NOT_NULL);
    return BASE32.encodeToString(bytes).toLowerCase().replace("=", "");
  }

  /**
   * @param bytes  to transform
   * @param offset offset to start from
   * @param size   size to use from offset
   * @return the Base 32 representation Without Padding representation
   * @throws CcsInvalidArgumentRuntimeException if argument is not compatible
   */
  public static String getBase32(final byte[] bytes, final int offset, final int size) {
    ParametersChecker.checkParameter(ARGUMENT_NULL_NOT_ALLOWED, bytes, NOT_NULL);
    return StringUtils.newStringUtf8(BASE32.encode(bytes, offset, size)).toLowerCase().replace("=", "");
  }

  /**
   * @param bytes to transform
   * @return the Base 64 Without Padding representation
   * @throws CcsInvalidArgumentRuntimeException if argument is not compatible
   */
  public static String getBase64(final byte[] bytes) {
    ParametersChecker.checkParameter(ARGUMENT_NULL_NOT_ALLOWED, bytes, NOT_NULL);
    return BASE64.encodeToString(bytes);
  }

  /**
   * @param bytes  to transform
   * @param offset offset to start from
   * @param size   size to use from offset
   * @return the Base 64 Without Padding representation
   * @throws CcsInvalidArgumentRuntimeException if argument is not compatible
   */
  public static String getBase64(final byte[] bytes, final int offset, final int size) {
    ParametersChecker.checkParameter(ARGUMENT_NULL_NOT_ALLOWED, bytes, NOT_NULL);
    if (offset > 0 || size < bytes.length) {
      return BASE64.encodeToString(Arrays.copyOfRange(bytes, offset, size));
    }
    return BASE64.encodeToString(bytes);
  }

  /**
   * @param bytes to transform
   * @return the Base 64 Without Padding representation
   * @throws CcsInvalidArgumentRuntimeException if argument is not compatible
   */
  public static String getBase64Padding(final byte[] bytes) {
    ParametersChecker.checkParameter(ARGUMENT_NULL_NOT_ALLOWED, bytes, NOT_NULL);
    return BASE64STANDARD.encodeToString(bytes);
  }

  /**
   * @param bytes  to transform
   * @param offset offset to start from
   * @param size   size to use from offset
   * @return the Base 64 Without Padding representation
   * @throws CcsInvalidArgumentRuntimeException if argument is not compatible
   */
  public static String getBase64Padding(final byte[] bytes, final int offset, final int size) {
    ParametersChecker.checkParameter(ARGUMENT_NULL_NOT_ALLOWED, bytes, NOT_NULL);
    if (offset > 0 || size < bytes.length) {
      return BASE64STANDARD.encodeToString(Arrays.copyOfRange(bytes, offset, size));
    }
    return BASE64STANDARD.encodeToString(bytes);
  }

  /**
   * @param bytes to transform
   * @return the Base 64 Without Padding representation
   * @throws CcsInvalidArgumentRuntimeException if argument is not compatible
   */
  public static String getBase64Url(final byte[] bytes) {
    ParametersChecker.checkParameter(ARGUMENT_NULL_NOT_ALLOWED, bytes, NOT_NULL);
    return BASE64URL.encodeToString(bytes);
  }

  /**
   * @param bytes  to transform
   * @param offset offset to start from
   * @param size   size to use from offset
   * @return the Base 64 Without Padding representation
   * @throws CcsInvalidArgumentRuntimeException if argument is not compatible
   */
  public static String getBase64Url(final byte[] bytes, final int offset, final int size) {
    ParametersChecker.checkParameter(ARGUMENT_NULL_NOT_ALLOWED, bytes, NOT_NULL);
    if (offset > 0 || size < bytes.length) {
      return BASE64URL.encodeToString(Arrays.copyOfRange(bytes, offset, size));
    }
    return BASE64URL.encodeToString(bytes);
  }

  /**
   * @param base16 to transform
   * @return the byte from Base 16 Without Padding
   * @throws CcsInvalidArgumentRuntimeException if argument is not compatible
   */
  public static byte[] getFromBase16(final String base16) {
    ParametersChecker.checkParameter(ARGUMENT_NULL_NOT_ALLOWED, base16);
    return BASE16.decode(base16);
  }

  /**
   * @param base32 to transform
   * @return the byte from Base 32 Without Padding
   * @throws CcsInvalidArgumentRuntimeException if argument is not compatible
   */
  public static byte[] getFromBase32(final String base32) {
    ParametersChecker.checkParameter(ARGUMENT_NULL_NOT_ALLOWED, base32);
    return BASE32.decode(base32);
  }

  /**
   * @param base64 to transform
   * @return the byte from Base 64 Without Padding
   * @throws CcsInvalidArgumentRuntimeException if argument is not compatible
   */
  public static byte[] getFromBase64(final String base64) {
    ParametersChecker.checkParameter(ARGUMENT_NULL_NOT_ALLOWED, base64);
    return BASE64_DEC.decode(base64);
  }

  /**
   * @param base64 to transform
   * @return the byte from Base 64 Without Padding
   * @throws CcsInvalidArgumentRuntimeException if argument is not compatible
   */
  public static byte[] getFromBase64Padding(final String base64) {
    return getFromBase64(base64);
  }

  /**
   * @param base64 to transform
   * @return the byte from Base 64 Without Padding
   * @throws CcsInvalidArgumentRuntimeException if argument is not compatible
   */
  public static byte[] getFromBase64Url(final String base64) {
    ParametersChecker.checkParameter(ARGUMENT_NULL_NOT_ALLOWED, base64);
    return BASE64URL_DEC.decode(base64);
  }
}
