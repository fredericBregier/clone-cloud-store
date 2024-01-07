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

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.common.standard.system.SysErrLogger;
import io.clonecloudstore.common.standard.system.SystemRandomSecure;

import static io.clonecloudstore.common.standard.properties.StandardProperties.getCcsMachineId;

/**
 * Internal class for Process Id, Mac Address and Jvm Id (PID + MacAddress)
 */
public final class JvmProcessMacIds {
  public static final long MODULO = 281474976710656L;
  public static final long FACTOR_3_BYTES = 8388608;
  /**
   * Definition for Machine Id replacing MAC address
   */
  private static final Pattern MACHINE_ID_PATTERN = Pattern.compile("^(?:[0-9a-fA-F][:-]?){6,8}$");
  private static final int MACHINE_ID_LEN = 8;
  /**
   * MAX value on 4 bytes (64 system use 2^31-1 id, in fact shall be 4 M)
   */
  private static final int MAX_PID = 0x7FFFFFFF;
  private static final int BYTE_FILTER = 0xFF;
  private static final short BYTE_SIZE = 8;
  private static final Object[] EMPTY_OBJECTS = new Object[0];
  private static final Class<?>[] EMPTY_CLASSES = new Class<?>[0];
  private static final Pattern COMPILE = Pattern.compile("[:-]");
  private static final byte[] EMPTY_BYTES = {};

  private static final int JVM_PID;
  private static byte[] mac;
  private static long macLong;
  private static int macInt;
  private static byte jvmByteId;
  private static int jvmIntegerId;
  private static long jvmLongId;
  private static byte[] jvmBytesMacPid;

  static {
    JVM_PID = jvmProcessId();
    mac = macAddress();
    macLong = macAddressAsLong();
    macInt = macAddressAsInt();
    jvmIntegerId = jvmInstanceIdAsInteger();
    jvmByteId = jvmInstanceIdAsByte();
    jvmLongId = jvmInstanceIdAsLong();
    jvmBytesMacPid = jvmMacPidAsBytes();
  }

  private JvmProcessMacIds() {
  }

  /**
   * @return The JVM PID as integer
   */
  public static int getJvmPID() {
    return JVM_PID;
  }

  /**
   * @return the Mac address as byte array
   */
  public static synchronized byte[] getMac() {
    return mac;
  }

  /**
   * @return the Mac and PID as byte array of size 6
   */
  public static byte[] getMacPid() {
    return jvmBytesMacPid;
  }

  /**
   * Up to the 8 first bytes will be used. If Null or less than 6 bytes, extra
   * bytes will be randomly generated, up to 6 bytes.
   *
   * @param mac the MAC address in byte format (up to the 8 first
   *            bytes will be used)
   */
  public static synchronized void setMac(final byte[] mac) {
    if (mac == null) {
      JvmProcessMacIds.mac = SystemRandomSecure.getRandom(MACHINE_ID_LEN);
    } else {
      if (mac.length < 6) {
        JvmProcessMacIds.mac = Arrays.copyOf(mac, 6);
        final var secureRandom = SystemRandomSecure.getSecureRandomSingleton();
        for (var i = mac.length; i < 6; i++) {
          JvmProcessMacIds.mac[i] = (byte) secureRandom.nextInt(256);
        }
      } else {
        JvmProcessMacIds.mac = Arrays.copyOf(mac, Math.min(mac.length, MACHINE_ID_LEN));
      }
    }
    internalInitialize();
  }

  private static void internalInitialize() {
    macLong = macAddressAsLong();
    macInt = macAddressAsInt();
    jvmIntegerId = jvmInstanceIdAsInteger();
    jvmByteId = jvmInstanceIdAsByte();
    jvmLongId = jvmInstanceIdAsLong();
    jvmBytesMacPid = jvmMacPidAsBytes();
  }

  /**
   * @return MAC address as long
   */
  private static long macAddressAsLong() {
    var value = (long) (mac[5] & BYTE_FILTER) << 40 | (long) (mac[4] & BYTE_FILTER) << 32 |
        (long) (mac[3] & BYTE_FILTER) << 24 | (long) (mac[2] & BYTE_FILTER) << 16 | (long) (mac[1] & BYTE_FILTER) << 8 |
        mac[0] & BYTE_FILTER;
    if (mac.length > 6) {
      value |= (long) (mac[6] & BYTE_FILTER) << 48;
      if (mac.length > 7) {
        return (long) (mac[7] & BYTE_FILTER) << 56 | value;
      }
    }
    return value;
  }

  /**
   * @return MAC address as int (truncated to 4 bytes instead of 8)
   */
  private static int macAddressAsInt() {
    return (mac[3] & BYTE_FILTER) << 24 | (mac[2] & BYTE_FILTER) << 16 | (mac[1] & BYTE_FILTER) << 8 |
        mac[0] & BYTE_FILTER;
  }

  /**
   * Use both PID and MAC address but as 4 bytes hash
   *
   * @return one id as much as possible unique
   */
  private static int jvmInstanceIdAsInteger() {
    final var id = 31L * JVM_PID + macInt;
    return Long.hashCode(id);
  }

  /**
   * Use both PID and MAC address but as 8 bites hash
   *
   * @return one id as much as possible unique
   */
  private static byte jvmInstanceIdAsByte() {
    return (byte) (Integer.hashCode(jvmIntegerId) & BYTE_FILTER);
  }

  /**
   * Use both PID (2 bytes at must) and MAC address
   *
   * @return one id as much as possible unique
   */
  private static long jvmInstanceIdAsLong() {
    return (macLong & 0xFFFFFFFFFFFFL) + ((long) JVM_PID << 6 * 8 & 0xFFFF);
  }

  private static byte[] jvmMacPidAsBytes() {
    final var bytes = new byte[6];
    var mod = (JvmProcessMacIds.getMacLong() * FACTOR_3_BYTES + JvmProcessMacIds.getJvmPID()) % MODULO;
    for (var pos = 0; pos < 6; pos++) {
      bytes[pos] = (byte) (mod & BYTE_FILTER);
      mod >>>= BYTE_SIZE;
    }
    return bytes;
  }

  /**
   * @return the Mac address truncated to long
   */
  public static long getMacLong() {
    return macLong;
  }

  /**
   * @return the Mac address truncated to integer
   */
  public static int getMacInt() {
    return macInt;
  }

  /**
   * @return The JVM Id (PID + MacAddress) truncated as 1 byte
   */
  public static byte getJvmByteId() {
    return jvmByteId;
  }

  /**
   * @return The JVM Id (PID + MacAddress) truncated as integer
   */
  public static int getJvmIntegerId() {
    return jvmIntegerId;
  }

  /**
   * @return The JVM Id (PID + MacAddress) truncated as long
   */
  public static long getJvmLongId() {
    return jvmLongId;
  }

  /**
   * @return the JVM Process ID
   */
  private static int jvmProcessId() {
    // Note: may fail in some JVM implementations
    // something like '<pid>@<hostname>', at least in SUN / Oracle JVMs
    try {
      final var loader = getSystemClassLoader();
      String value;
      value = jvmProcessIdManagementFactory(loader);
      final var atIndex = value.indexOf('@');
      if (atIndex >= 0) {
        value = value.substring(0, atIndex);
      }
      var processId = -1;
      processId = parseProcessId(processId, value);
      if (processId > 0) {
        return processId;
      }
    } catch (final RuntimeException e) {// NOSONAR intentional
      SysErrLogger.FAKE_LOGGER.syserr(e);
    }
    return SystemRandomSecure.getSecureRandomSingleton().nextInt(MAX_PID);
  }

  private static ClassLoader getSystemClassLoader() {
    return ClassLoader.getSystemClassLoader();
  }

  /**
   * @return the processId as String
   */
  private static String jvmProcessIdManagementFactory(final ClassLoader loader) {
    String value;
    try {
      // Invoke
      // java.lang.management.ManagementFactory.getRuntimeMXBean().getName()
      final var mgmtFactoryType = Class.forName("java.lang.management.ManagementFactory", true, loader);
      final var runtimeMxBeanType = Class.forName("java.lang.management.RuntimeMXBean", true, loader);

      final var getRuntimeMXBean = mgmtFactoryType.getMethod("getRuntimeMXBean", JvmProcessMacIds.EMPTY_CLASSES);
      final var bean = getRuntimeMXBean.invoke(null, JvmProcessMacIds.EMPTY_OBJECTS);
      final var getName = runtimeMxBeanType.getDeclaredMethod("getName", JvmProcessMacIds.EMPTY_CLASSES);
      value = (String) getName.invoke(bean, JvmProcessMacIds.EMPTY_OBJECTS);
    } catch (final Exception e) {
      SysErrLogger.FAKE_LOGGER.syserr("Unable to get PID: " + e.getMessage());
      value = "";
    }
    return value;
  }

  /**
   * @return the processId
   */
  private static int parseProcessId(final int oldProcessId, final String customProcessId) {
    var processId = oldProcessId;
    try {
      processId = Integer.parseInt(customProcessId);
    } catch (final NumberFormatException e) {
      // Malformed input.
    }
    if (processId <= 0 || processId > MAX_PID) {
      processId = SystemRandomSecure.getSecureRandomSingleton().nextInt(MAX_PID);
    }
    return processId;
  }

  /**
   * @return the mac address if possible, else random values
   */
  private static byte[] macAddress() {
    try {
      byte[] machineId = null;
      final var customMachineId = getCcsMachineId();
      if (ParametersChecker.isNotEmpty(customMachineId) && MACHINE_ID_PATTERN.matcher(customMachineId).matches()) {
        machineId = parseMachineId(customMachineId);
      }

      if (machineId == null) {
        machineId = defaultMachineId();
      }
      return machineId;
    } catch (final RuntimeException e) {// NOSONAR intentional
      return SystemRandomSecure.getRandom(MACHINE_ID_LEN);
    }
  }

  private static byte[] parseMachineId(String value) {
    // Strip separators.
    value = COMPILE.matcher(value).replaceAll("");

    final var len = value.length();
    final var lenJ = len / 2;
    final var machineId = new byte[lenJ];
    for (int j = 0, i = 0; i < len && j < lenJ; i += 2, j++) {
      machineId[j] = (byte) Integer.parseInt(value.substring(i, i + 2), 16);
    }
    return machineId;
  }

  private static Map<NetworkInterface, InetAddress> getAvailableNetworkInterfaces() {
    // Retrieve the list of available network interfaces.
    final Map<NetworkInterface, InetAddress> ifaces = new LinkedHashMap<>();
    try {
      for (final var i = NetworkInterface.getNetworkInterfaces(); i.hasMoreElements(); ) {
        final var iface = i.nextElement();
        if (!iface.isUp() || iface.isLoopback()) {
          continue;
        }
        var addresses = iface.getInetAddresses();
        if (addresses == null) {
          addresses = Collections.enumeration(Collections.emptyList());
        }
        if (addresses.hasMoreElements()) {
          final var a = addresses.nextElement();
          if (!a.isLoopbackAddress() && !a.isAnyLocalAddress() && !a.isLinkLocalAddress()) {
            ifaces.put(iface, a);
          }
        }
      }
    } catch (final SocketException ignored) {
      // nothing
    }
    return ifaces;
  }

  private static byte[] findBestMacAddr(final Map<NetworkInterface, InetAddress> ifaces, InetAddress bestInetAddr) {
    var bestMacAddr = EMPTY_BYTES;
    for (final var entry : ifaces.entrySet()) {
      final var iface = entry.getKey();
      final var inetAddr = entry.getValue();
      byte[] macAddr = null;
      boolean cont = iface.isVirtual();
      if (!cont) {
        try {
          macAddr = iface.getHardwareAddress();
        } catch (final SocketException e) {
          cont = true;
        }
      }
      if (cont) {
        continue;
      }
      var replace = false;
      var res = compareAddresses(bestMacAddr, macAddr);
      if (res < 0) {
        // Found a better MAC address.
        replace = true;
      } else if (res == 0) {
        // Two MAC addresses are of pretty much same quality.
        res = compareAddresses(bestInetAddr, inetAddr);
        if (res < 0 || res == 0 && bestMacAddr.length < macAddr.length) {
          // Found a MAC address with better INET address.
          // Cannot tell the difference. Choose the longer one.
          replace = true;
        }
      }

      if (replace) {
        bestMacAddr = macAddr;
        bestInetAddr = inetAddr;
      }
    }
    return bestMacAddr;
  }

  private static byte[] defaultMachineId() {
    // Find the best MAC address available.
    final InetAddress bestInetAddr;
    try {
      bestInetAddr = InetAddress.getByAddress(new byte[]{127, 0, 0, 1});
    } catch (final UnknownHostException e) {
      // Never happens.
      throw new IllegalArgumentException(e);
    }
    // Retrieve the list of available network interfaces.
    final var ifaces = getAvailableNetworkInterfaces();
    var bestMacAddr = findBestMacAddr(ifaces, bestInetAddr);
    if (bestMacAddr == EMPTY_BYTES) {
      bestMacAddr = SystemRandomSecure.getRandom(MACHINE_ID_LEN);
    }
    return bestMacAddr;
  }

  /**
   * @return positive - current is better, 0 - cannot tell from MAC addr,
   * negative - candidate is better.
   */
  private static int compareAddresses(final byte[] current, final byte[] candidate) {
    if (candidate == null) {
      return 1;
    }
    // Must be EUI-48 or longer.
    if (candidate.length < 6) {
      return 1;
    }
    // Must not be filled with only 0 and 1.
    var onlyZeroAndOne = true;
    for (final var b : candidate) {
      if (b != 0 && b != 1) {
        onlyZeroAndOne = false;
        break;
      }
    }
    if (onlyZeroAndOne) {
      return 1;
    }
    // Must not be a multicast address
    if ((candidate[0] & 1) != 0) {
      return 1;
    }
    // Current is empty
    if (current.length == 0) {
      return -1;
    }
    // Prefer globally unique address.
    if ((candidate[0] & 2) == 0) {
      if ((current[0] & 2) == 0) {
        // Both current and candidate are globally unique addresses.
        return 0;
      } else {
        // Only current is globally unique.
        return -1;
      }
    } else {
      if ((current[0] & 2) == 0) {
        // Only candidate is globally unique.
        return 1;
      } else {
        // Both current and candidate are non-unique.
        return 0;
      }
    }
  }

  /**
   * @return positive - current is better, 0 - cannot tell, negative -
   * candidate
   * is better
   */
  private static int compareAddresses(final InetAddress current, final InetAddress candidate) {
    return scoreAddress(current) - scoreAddress(candidate);
  }

  private static int scoreAddress(final InetAddress addr) {
    if (addr.isAnyLocalAddress()) {
      return 0;
    }
    if (addr.isMulticastAddress()) {
      return 1;
    }
    if (addr.isLinkLocalAddress()) {
      return 2;
    }
    if (addr.isSiteLocalAddress()) {
      return 3;
    }

    return 4;
  }
}
