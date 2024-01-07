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

import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class JvmProcessMacIdsTest {

  @Test
  void checkValues() {
    final var mac = JvmProcessMacIds.getMac();
    final var macL = JvmProcessMacIds.getMacLong();
    long value = 0;
    for (var i = 0; i < mac.length; i++) {
      value <<= 8;
      value |= mac[mac.length - i - 1] & 0xFF;
    }
    assertEquals(macL, value);
    Log.infof("%d %d %d %d %d", JvmProcessMacIds.getJvmPID(), JvmProcessMacIds.getMacInt(),
        JvmProcessMacIds.getJvmIntegerId(), JvmProcessMacIds.getJvmByteId(), JvmProcessMacIds.getJvmLongId());
    assertTrue(JvmProcessMacIds.getJvmPID() > 0);
    assertTrue(JvmProcessMacIds.getMacInt() != 0);
    JvmProcessMacIds.getJvmIntegerId();
    assertTrue(JvmProcessMacIds.getJvmLongId() > 0);
    JvmProcessMacIds.setMac(mac);
    final var mac2 = JvmProcessMacIds.getMac();
    assertArrayEquals(mac, mac2);
    JvmProcessMacIds.setMac(null);
    assertFalse(Arrays.equals(mac, JvmProcessMacIds.getMac()));
    JvmProcessMacIds.setMac(mac);
    final var partial = Arrays.copyOf(mac, 4);
    JvmProcessMacIds.setMac(partial);
    assertFalse(Arrays.equals(mac, JvmProcessMacIds.getMac()));
    assertFalse(Arrays.equals(partial, JvmProcessMacIds.getMac()));
    JvmProcessMacIds.setMac(mac);
  }
}
