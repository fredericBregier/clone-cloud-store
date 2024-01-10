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

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.google.common.base.Strings;
import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import org.apache.commons.lang3.StringUtils;

/**
 * Checker for Parameters <br>
 * <br>
 * Can be used for String (testing also emptiness) and for general Object.<br>
 * For null String only, use the special method.
 */
public final class ParametersChecker {

  // Default ASCII for Param check
  private static final Pattern UNPRINTABLE_PATTERN = Pattern.compile("[\\p{Cntrl}]");
  private static final Pattern INVALID_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9/_ .\\-]");
  private static final Pattern SPACE_UNDERSCORE_PATTERN = Pattern.compile("[\\s_]+");
  private static final Pattern MINUS_PATTERN = Pattern.compile("\\-+");

  private static final List<String> RULES = new ArrayList<>();

  // default parameters for XML check
  private static final String CDATA_TAG_UNESCAPED = "<![CDATA[";
  private static final String CDATA_TAG_ESCAPED = "&lt;![CDATA[";
  private static final String ENTITY_TAG_UNESCAPED = "<!ENTITY";
  private static final String ENTITY_TAG_ESCAPED = "&lt;!ENTITY";
  // default parameters for Javascript check
  private static final String SCRIPT_TAG_UNESCAPED = "<script>";
  private static final String SCRIPT_TAG_ESCAPED = "&lt;script&gt;";
  private static final String SQL_COMMA_ESCAPED = ";";
  public static final int BUCKET_LENGTH = 63;
  // FIXME Once clientId key size known, will be extracted from BUCKET_LENGTH - 1
  public static final int BUCKET_NOSITE_LENGTH = 60;
  public static final int OBJECT_LENGTH = 1024;
  public static final int SITE_LENGTH = 256;
  private static final Pattern BUCKET_NAME_PATTERN = Pattern.compile("^[0-9a-z\\-]{3," + BUCKET_LENGTH + "}$");
  private static final Pattern OBJECT_NAME_PATTERN = Pattern.compile("^[0-9a-zA-Z_./\\-]{1," + OBJECT_LENGTH + "}$");
  public static final String INVALID_INPUT = "Invalid input";

  public static final String INVALID_URI = "Invalid uri [%s]";
  public static final String ACCENTS = "āăąēîïĩíĝġńñšŝśûůŷ";

  static {
    RULES.add(CDATA_TAG_UNESCAPED);
    RULES.add(CDATA_TAG_ESCAPED);
    RULES.add(ENTITY_TAG_UNESCAPED);
    RULES.add(ENTITY_TAG_ESCAPED);
    RULES.add(SCRIPT_TAG_UNESCAPED);
    RULES.add(SCRIPT_TAG_ESCAPED);
    RULES.add(SQL_COMMA_ESCAPED);
  }

  private ParametersChecker() {
    // empty
  }

  /**
   * Check if any parameter are null or empty and if so, throw an
   * CcsInvalidArgumentRuntimeException
   *
   * @param errorMessage the error message
   * @param parameters   parameters to be checked
   * @throws CcsInvalidArgumentRuntimeException if null or empty
   */
  public static void checkParameter(final String errorMessage, final Object... parameters)
      throws CcsInvalidArgumentRuntimeException {
    if (parameters == null) {
      throw new CcsInvalidArgumentRuntimeException(errorMessage);
    }
    for (final var parameter : parameters) {
      if (parameter == null ||
          parameter instanceof String param && (Strings.isNullOrEmpty(param) || param.trim().isEmpty())) {
        throw new CcsInvalidArgumentRuntimeException(errorMessage);
      }
    }
  }

  /**
   * Check if any parameter are null and if so, throw an
   * CcsInvalidArgumentRuntimeException
   *
   * @param errorMessage the error message
   * @param parameters   parameters to be checked
   * @throws CcsInvalidArgumentRuntimeException if null
   */
  public static void checkParameterNullOnly(final String errorMessage, final Object... parameters)
      throws CcsInvalidArgumentRuntimeException {
    if (parameters == null) {
      throw new CcsInvalidArgumentRuntimeException(errorMessage);
    }
    for (final var parameter : parameters) {
      if (parameter == null) {
        throw new CcsInvalidArgumentRuntimeException(errorMessage);
      }
    }
  }

  /**
   * Check if an integer parameter is greater or equals to minValue
   *
   * @param name     name of the variable
   * @param variable the value of variable to check
   * @param minValue the min value
   * @throws CcsInvalidArgumentRuntimeException if invalid
   */
  public static void checkValue(final String name, final long variable, final long minValue)
      throws CcsInvalidArgumentRuntimeException {
    if (variable < minValue) {
      throw new CcsInvalidArgumentRuntimeException("Parameter " + name + " is less than " + minValue);
    }
  }

  /**
   * Check external argument to avoid Path Traversal attack
   *
   * @param value to check
   * @throws CcsInvalidArgumentRuntimeException if invalid
   */
  public static void checkSanityString(final String value) throws CcsInvalidArgumentRuntimeException {
    if (isEmpty(value)) {
      return;
    }
    if (UNPRINTABLE_PATTERN.matcher(value).find()) {
      throw new CcsInvalidArgumentRuntimeException(INVALID_INPUT);
    }
    for (final var rule : RULES) {
      if (rule != null && value.contains(rule)) {
        throw new CcsInvalidArgumentRuntimeException("Invalid tag sanity check");
      }
    }
  }

  /**
   * Check external argument (null is considered as correct)
   *
   * @param objects the array to check (toString)
   * @throws CcsInvalidArgumentRuntimeException if invalid
   */
  public static void checkSanity(final Object... objects) throws CcsInvalidArgumentRuntimeException {
    for (final var field : objects) {
      checkSanityString(field.toString());
    }
  }

  /**
   * Check external argument (null is considered as correct)
   *
   * @param strings the String array to check
   * @throws CcsInvalidArgumentRuntimeException if invalid
   */
  public static void checkSanityString(final String... strings) throws CcsInvalidArgumentRuntimeException {
    for (final var field : strings) {
      checkSanityString(field);
    }
  }

  /**
   * Check external argument to avoid Path Traversal attack
   *
   * @param bucketName to check
   * @throws CcsInvalidArgumentRuntimeException if invalid
   */
  public static void checkSanityBucketName(final String bucketName) throws CcsInvalidArgumentRuntimeException {
    if (!BUCKET_NAME_PATTERN.matcher(bucketName).find()) {
      throw new CcsInvalidArgumentRuntimeException(INVALID_INPUT);
    }
  }

  /**
   * Check external argument to avoid Path Traversal attack
   *
   * @param objectName to check
   * @throws CcsInvalidArgumentRuntimeException if invalid
   */
  public static void checkSanityObjectName(final String objectName) throws CcsInvalidArgumentRuntimeException {
    if (!OBJECT_NAME_PATTERN.matcher(objectName).find()) {
      throw new CcsInvalidArgumentRuntimeException(INVALID_INPUT);
    }
  }

  /**
   * Check if any parameter are null or empty and if so, return true
   *
   * @param parameters set of parameters
   * @return True if any is null or empty or containing only spaces
   */
  public static boolean isEmpty(final Object... parameters) {
    if (parameters == null) {
      return true;
    }
    for (final var parameter : parameters) {
      if (parameter == null ||
          parameter instanceof String param && (Strings.isNullOrEmpty(param) || param.trim().isEmpty())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check if any parameter are null or empty and if so, return false
   *
   * @param parameters set of parameters
   * @return True if not null and not empty neither containing only spaces
   */
  public static boolean isNotEmpty(final Object... parameters) {
    if (parameters == null) {
      return false;
    }
    for (final var parameter : parameters) {
      if (parameter == null ||
          parameter instanceof String param && (Strings.isNullOrEmpty(param) || param.trim().isEmpty())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check if at least one parameter is not null and not empty and if so, return true
   *
   * @param parameters set of parameters
   * @return True if at least one is not null and not empty neither containing only spaces
   */
  public static boolean hasNotEmpty(final Object... parameters) {
    if (parameters == null) {
      return false;
    }
    for (final var parameter : parameters) {
      if (parameter != null &&
          (!(parameter instanceof String param) || !Strings.isNullOrEmpty(param) && !param.trim().isEmpty())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check external argument to avoid Path Traversal attack
   *
   * @param uri to check
   * @throws CcsInvalidArgumentRuntimeException if invalid
   */
  public static void checkSanityUri(final String uri) throws CcsInvalidArgumentRuntimeException {
    try {
      new URI(uri);
    } catch (URISyntaxException e) {
      throw new CcsInvalidArgumentRuntimeException(String.format(INVALID_URI, uri));
    }
  }

  public static String getSanitizedName(final String objectName) {
    if (isEmpty(objectName)) {
      return null;
    }
    final var decoded = urlDecodePathParam(objectName);
    checkSanityString(objectName);
    var name = decoded.replace('\\', '/').replace("//", "/");
    name = StringUtils.stripAccents(name);
    name = SPACE_UNDERSCORE_PATTERN.matcher(name).replaceAll(" ");
    name = INVALID_CHAR_PATTERN.matcher(name).replaceAll("");
    name = MINUS_PATTERN.matcher(name).replaceAll("-");
    name = name.trim();
    name = SPACE_UNDERSCORE_PATTERN.matcher(name).replaceAll("_");
    if (name.startsWith("/")) {
      name = name.substring(1);
    }
    return name;
  }

  public static String urlDecodePathParam(final String path) {
    return URLDecoder.decode(path, StandardCharsets.UTF_8);
  }
}
