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

package io.clonecloudstore.test.stream;

import java.io.OutputStream;

/**
 * Void OutputStream as /dev/null
 */
public class VoidOutputStream extends OutputStream {
  private long size = 0;

  @Override
  public void write(final int i) {
    size++;
  }

  @Override
  public void write(final byte[] bytes) {
    size += bytes != null ? bytes.length : 0;
  }

  @Override
  public void write(final byte[] bytes, final int off, final int len) {
    size += bytes != null ? len : 0;
  }

  @Override
  public void flush() {
    // Empty
  }

  @Override
  public void close() {
    // Empty
  }

  public long getSize() {
    return size;
  }
}
