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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.ZstdOutputStream;

/**
 * Zstd InputStream: takes an InputStream as entry and give back a compressed InputStream
 */
public class ZstdCompressInputStream extends AbstractCommonInputStream {

  /**
   * Default constructor with flush by packet
   */
  public ZstdCompressInputStream(final InputStream inputStream) throws IOException {
    this(inputStream, -1);
  }

  @Override
  protected OutputStream getNewOutputStream(Object level) throws IOException {
    var zstdOutputStream = new ZstdOutputStream(pipedOutputStream, RecyclingBufferPool.INSTANCE, (int) level);
    zstdOutputStream.setChecksum(false);
    zstdOutputStream.setCloseFrameOnFlush(true);
    return zstdOutputStream;
  }

  public ZstdCompressInputStream(final InputStream inputStream, final int level) throws IOException {
    super(inputStream, level);
  }

  public long getSizeRead() {
    return sizeRead;
  }

  public long getSizeCompressed() {
    return sizeOutput;
  }
}
