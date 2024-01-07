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
import java.util.concurrent.atomic.AtomicInteger;

import io.clonecloudstore.common.standard.exception.CcsInvalidArgumentRuntimeException;
import io.clonecloudstore.common.standard.system.BaseXx;

/**
 * UUID Generator but limited to 1 Long (64 bits), compatible multi instances (through the PID,
 * but not multi servers, neither multi-sites due to lack of Mac address/Platform Id.
 * <br>
 * <br>
 * Inspired from com.groupon locality-uuid which used combination of internal
 * counter value, process id and
 * Timestamp. see https://github.com/groupon/locality-uuid.java <br>
 * <br>
 * Benchmark shows about 20 millions/s generated LongUuid, 8 M/s getId().
 */
public final class LongUuid {
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
  private static final AtomicInteger COUNTER = new AtomicInteger(MIN_COUNTER);
  /**
   * Byte size of UUID
   */
  private static final int UUID_SIZE = 8;


  /**
   * real UUID
   */
  private final byte[] uuid = {0, 0, 0, 0, 0, 0, 0, 0};

  /**
   * Constructor that generates a new UUID using the current process id and
   * MAC address, timestamp and a counter
   */
  public LongUuid() {
    this(getLongUuid());
  }

  public LongUuid(final long value) {
    uuid[0] = (byte) (value >> 56);
    uuid[1] = (byte) (value >> 48);
    uuid[2] = (byte) (value >> 40);
    uuid[3] = (byte) (value >> 32);
    uuid[4] = (byte) (value >> 24);
    uuid[5] = (byte) (value >> 16);
    uuid[6] = (byte) (value >> 8);
    uuid[7] = (byte) value;
  }

  /**
   * @return a new Uuid as long
   */
  public static long getLongUuid() {
    final var time = System.currentTimeMillis();
    // atomically
    final var count = COUNTER.getAndIncrement() % MAX_COUNTER;
    // Jvmd Id on 4 first bits
    // Timestamp on 40 bits (2^40 ms = 35 years rolling)
    // Count on 20 bits => 2^20 (1M / ms)
    var uuidAsLong = (JvmProcessMacIds.getJvmByteId() & 0xF0L) << 56;
    uuidAsLong |= (time & 0xFFFFFFFFFFL) << 20;
    uuidAsLong |= count & 0xFFFFFL;
    return uuidAsLong;
  }

  static int getCounter() {
    return COUNTER.getAndIncrement() % MAX_COUNTER;
  }

  /**
   * Constructor that takes a byte array as UUID's content
   *
   * @param bytes UUID content
   */
  public LongUuid(final byte[] bytes) {
    if (bytes.length != UUID_SIZE) {
      throw new CcsInvalidArgumentRuntimeException("Attempted to parse malformed UUID: " + Arrays.toString(bytes));
    }
    System.arraycopy(bytes, 0, uuid, 0, UUID_SIZE);
  }

  /**
   * Constructor that takes a String representation of a UUID in Base 16
   *
   * @param idsource UUID content as String
   */
  public LongUuid(final String idsource) {
    final var id = idsource.trim();

    if (id.length() != UUID_SIZE * 2) {
      throw new CcsInvalidArgumentRuntimeException("Attempted to parse malformed UUID: " + id);
    }
    System.arraycopy(BaseXx.getFromBase16(id), 0, uuid, 0, UUID_SIZE);
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

  /**
   * Extract process id from raw UUID bytes and return as int
   *
   * @return id of process that generated the UUID
   */
  public int getProcessId() {
    return uuid[0] >> 4 & 0x0F;
  }

  /**
   * Extract timestamp from raw UUID bytes and return as int
   *
   * @return millisecond UTC timestamp from generation of the UUID
   */
  public long getTimestamp() {
    long time;
    time = ((long) uuid[0] & 0x0F) << 36;
    time |= ((long) uuid[1] & 0xFF) << 28;
    time |= ((long) uuid[2] & 0xFF) << 20;
    time |= ((long) uuid[3] & 0xFF) << 12;
    time |= ((long) uuid[4] & 0xFF) << 4;
    time |= ((long) uuid[5] & 0xF0) >> 4;
    return time;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(uuid);
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof LongUuid)) {
      return false;
    }
    return this == o || Arrays.equals(uuid, ((LongUuid) o).uuid);
  }

  /**
   * @return the Base 16 representation
   */
  @Override
  public String toString() {
    return getId();
  }

  /**
   * @return the Base 16 representation
   */
  public String getId() {
    return BaseXx.getBase16(uuid);
  }

  /**
   * @return the equivalent UUID as long
   */
  public long getLong() {
    var value = ((long) uuid[0] & 0xFF) << 56;
    value |= ((long) uuid[1] & 0xFF) << 48;
    value |= ((long) uuid[2] & 0xFF) << 40;
    value |= ((long) uuid[3] & 0xFF) << 32;
    value |= ((long) uuid[4] & 0xFF) << 24;
    value |= ((long) uuid[5] & 0xFF) << 16;
    value |= ((long) uuid[6] & 0xFF) << 8;
    value |= (long) uuid[7] & 0xFF;
    return value;
  }
}
