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

package io.clonecloudstore.common.quarkus;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.MethodName.class)
@QuarkusTest
class ApiFullServerDoubleTest extends ApiFullServerDoubleAbstract {
  @Test
  void check30PostInputStreamQuarkusDouble() {
    check30PostInputStreamQuarkusDoubleTest();
  }

  @Test
  void check31PostInputStreamQuarkusDoubleNoSize() {
    check31PostInputStreamQuarkusDoubleNoSizeTest();
  }

  @Test
  void check32PostInputStreamQuarkusShaDouble() {
    check32PostInputStreamQuarkusShaDoubleTest();
  }

  @Test
  void check33PostInputStreamQuarkusShaDoubleNoSize() {
    check33PostInputStreamQuarkusShaDoubleNoSizeTest();
  }

  @Test
  void check34GetInputStreamQuarkusDouble() {
    check34GetInputStreamQuarkusDoubleTest();
  }

  @Test
  void check35GetInputStreamQuarkusDoubleNoSize() {
    check35GetInputStreamQuarkusDoubleNoSizeTest();
  }

  @Test
  void check35WrongPostInputStreamQuarkusDouble() {
    check35WrongPostInputStreamQuarkusDoubleTest();
  }

  @Test
  void check36WrongPostInputStreamQuarkusNoSizeDouble() {
    check36WrongPostInputStreamQuarkusNoSizeDoubleTest();
  }

  @Test
  void check37WrongGetInputStreamQuarkusDouble() {
    check37WrongGetInputStreamQuarkusDoubleTest();
  }

  @Test
  void check38WrongGetInputStreamQuarkusNoSizeDouble() {
    check38WrongGetInputStreamQuarkusNoSizeDoubleTest();
  }

  @Test
  void check41PostInputStreamQuarkusDoubleCompressedIntra() {
    check41PostInputStreamQuarkusDoubleCompressedIntraTest();
  }

  @Test
  void check42PostInputStreamQuarkusDoubleNoSizeCompressedIntra() {
    check42PostInputStreamQuarkusDoubleNoSizeCompressedIntraTest();
  }

  @Test
  void check43PostInputStreamQuarkusShaDoubleCompressedIntra() {
    check43PostInputStreamQuarkusShaDoubleCompressedIntraTest();
  }

  @Test
  void check44PostInputStreamQuarkusShaDoubleNoSizeCompressedIntra() {
    check44PostInputStreamQuarkusShaDoubleNoSizeCompressedIntraTest();
  }

  @Test
  void check45WrongPostInputStreamQuarkusDoubleCompressedIntra() {
    check45WrongPostInputStreamQuarkusDoubleCompressedIntraTest();
  }

  @Test
  void check46WrongPostInputStreamQuarkusNoSizeDoubleCompressedIntra() {
    check46WrongPostInputStreamQuarkusNoSizeDoubleCompressedIntraTest();
  }

  @Test
  void check47WrongGetInputStreamQuarkusDoubleCompressedIntra() {
    check47WrongGetInputStreamQuarkusDoubleCompressedIntraTest();
  }

  @Test
  void check48WrongGetInputStreamQuarkusNoSizeDoubleCompressedIntra() {
    check48WrongGetInputStreamQuarkusNoSizeDoubleCompressedIntraTest();
  }

  @Test
  void check50GetInputStreamQuarkusDoubleCompressedIntra() {
    check50GetInputStreamQuarkusDoubleCompressedIntraTest();
  }

  @Test
  void check51GetInputStreamQuarkusDoubleNoSizeCompressedIntra() {
    check51GetInputStreamQuarkusDoubleNoSizeCompressedIntraTest();
  }
}
