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

package io.clonecloudstore.common.standard.guid;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.system.BaseXx;

/**
 * UUID Generator with 2 longs (128 bits), compatible multi instances (through the PID,
 * but not multi servers, neither multi-sites due to lack of Mac address/Platform Id.
 * <br>
 * <br>
 * Inspired from com.groupon locality-uuid which used combination of internal
 * counter value, process id and
 * Timestamp. see https://github.com/groupon/locality-uuid.java <br>
 * <br>
 * Benchmark shows about 20 millions/s generated GuidLike, 8 M/s getId().
 */
public final class GuidLike {
  /**
   * Bits size of Counter
   */
  private static final int SIZE_COUNTER = 20;
  /**
   * Min Counter value
   */
  private static final int MIN_COUNTER = 0;
  /**
   * Max Counter value
   */
  private static final int MAX_COUNTER = (1 << SIZE_COUNTER);
  /**
   * Counter part
   */
  private static final AtomicLong COUNTER = new AtomicLong(MIN_COUNTER);
  /**
   * Byte size of UUID
   */
  private static final int UUID_SIZE = 16;
  /**
   * GUID Like Base 32 size
   */
  public static final int UUID_B32_SIZE = 26;
  /**
   * GUID Like Base 32 size
   */
  private static final int UUID_B64_SIZE = 22;
  private static final short BYTE_MASK = 0xFF;
  private static final short BYTE_SIZE = 8;
  private static final short VERSION = 1;
  /**
   * real UUID
   */
  private final byte[] uuid;

  /**
   * @return Convenient method to get String GUID (similar to new GuidLike().getId())
   */
  public static String getGuid() {
    return BaseXx.getBase32(new GuidLike().uuid);
  }

  /**
   * Constructor that generates a new UUID using the current process id and
   * MAC address, timestamp and a counter
   */
  public GuidLike() {
    uuid = new byte[UUID_SIZE];
    // Platform and PID in 6 bytes
    // Header 1 byte
    System.arraycopy(JvmProcessMacIds.getMacPid(), 0, uuid, 7, 6);
    uuid[6] = VERSION & BYTE_MASK;
    // Counter with 3 bytes
    for (int pos = 13, valuei = (int) (COUNTER.getAndIncrement() % MAX_COUNTER); pos < 16;
         pos++, valuei >>>= BYTE_SIZE) {
      uuid[pos] = (byte) (valuei & BYTE_MASK);
    }
    // Timestamp with 6 bytes
    var value = System.currentTimeMillis();
    for (int pos = 0; pos < 6; pos++, value >>>= BYTE_SIZE) {
      uuid[pos] = (byte) (value & BYTE_MASK);
    }
  }

  public GuidLike(final long valueLow, final long valueHigh) {
    uuid = new byte[UUID_SIZE];
    for (int pos = 0, move = 56; pos < 7; pos++, move -= 8) {
      uuid[pos] = (byte) ((valueLow >> move) & BYTE_MASK);
    }
    uuid[7] = (byte) (valueLow & BYTE_MASK);
    for (int pos = 8, move = 56; pos < 15; pos++, move -= 8) {
      uuid[pos] = (byte) ((valueHigh >> move) & BYTE_MASK);
    }
    uuid[15] = (byte) (valueHigh & BYTE_MASK);
  }

  /**
   * Constructor that takes a byte array as UUID's content
   *
   * @param bytes UUID content
   */
  public GuidLike(final byte[] bytes) {
    if (bytes.length != UUID_SIZE) {
      throw new CcsInvalidArgumentRuntimeException("Attempted to parse malformed UUID: " + Arrays.toString(bytes));
    }
    uuid = new byte[UUID_SIZE];
    System.arraycopy(bytes, 0, uuid, 0, UUID_SIZE);
  }

  /**
   * Constructor that takes a String representation of a UUID in Base 16
   *
   * @param idsource UUID content as String
   */
  public GuidLike(final String idsource) {
    final var id = idsource.trim();
    uuid = new byte[UUID_SIZE];
    if (id.length() == UUID_SIZE * 2) {
      System.arraycopy(BaseXx.getFromBase16(id), 0, uuid, 0, UUID_SIZE);
    } else if (id.length() == UUID_B32_SIZE) {
      System.arraycopy(BaseXx.getFromBase32(id), 0, uuid, 0, UUID_SIZE);
    } else if (id.length() == UUID_B64_SIZE) {
      System.arraycopy(BaseXx.getFromBase64(id), 0, uuid, 0, UUID_SIZE);
    } else {
      throw new CcsInvalidArgumentRuntimeException("Attempted to parse malformed UUID: " + id);
    }
  }

  /**
   * @return (pseudo) time
   */
  public long getTime() {
    long value = uuid[0];
    for (var pos = 1; pos < 6; pos++) {
      value <<= BYTE_SIZE;
      value += uuid[pos];
    }
    return value;
  }

  /**
   * @return the LongUuid size in bytes
   */
  public static short getKeySize() {
    return UUID_SIZE;
  }

  /**
   * copy the uuid of this UUID, so that it can't be changed, and return it
   *
   * @return raw byte array of UUID
   */
  public byte[] getBytes() {
    return Arrays.copyOf(uuid, UUID_SIZE);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(uuid);
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof GuidLike)) {
      return false;
    }
    return this == o || Arrays.equals(uuid, ((GuidLike) o).uuid);
  }

  /**
   * @return the Base 32 representation
   */
  @Override
  public String toString() {
    return getId();
  }

  /**
   * @return the Base 32 representation
   */
  public String getId() {
    return toBase32();
  }

  public String toHex() {
    return BaseXx.getBase16(uuid);
  }

  public String toBase64() {
    return BaseXx.getBase64(uuid);
  }

  public String toBase32() {
    return BaseXx.getBase32(uuid);
  }

  /**
   * @return the equivalent Low UUID as long
   */
  public long getLongLow() {
    var value = ((long) uuid[0] & BYTE_MASK) << 56;
    for (int pos = 1, move = 48; pos < 7; pos++, move -= 8) {
      value |= ((long) uuid[pos] & BYTE_MASK) << move;
    }
    value |= (long) uuid[7] & BYTE_MASK;
    return value;
  }

  /**
   * @return the equivalent High UUID as long
   */
  public long getLongHigh() {
    var value = ((long) uuid[8] & BYTE_MASK) << 56;
    for (int pos = 9, move = 48; pos < 15; pos++, move -= 8) {
      value |= ((long) uuid[pos] & BYTE_MASK) << move;
    }
    value |= (long) uuid[15] & BYTE_MASK;
    return value;
  }
}
