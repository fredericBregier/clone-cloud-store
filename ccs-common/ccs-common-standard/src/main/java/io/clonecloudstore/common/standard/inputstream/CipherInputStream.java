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

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Cipher InputStream: takes an InputStream as entry and give back a crypt/decrypt InputStream
 */
public class CipherInputStream extends AbstractCommonInputStream {

  @Override
  protected OutputStream getNewOutputStream(Object cipher) {
    return new CipherOutputStream(pipedOutputStream, (Cipher) cipher);
  }

  /**
   * Constructor allowing to not flush on all packets
   */
  public CipherInputStream(final InputStream inputStream, final Cipher cipher) throws IOException {
    super(inputStream, cipher);
  }

  public long getSizeRead() {
    return sizeRead;
  }

  public long getSizeCipher() {
    return sizeOutput;
  }
}
