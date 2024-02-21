/*
 * Copyright (c) 2024. Clone Cloud Store (CCS), Contributors and Frederic Bregier
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

package io.clonecloudstore.common.quarkus.stream;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public interface ChunkInputStreamInterface extends Closeable {
  boolean nextChunk() throws IOException;

  boolean isChunksDone();

  long getAvailableChunkSize();

  long getChunkSize();

  int getCurrentPos();

  long getCurrentTotalRead();

  int read() throws IOException;

  int read(byte[] b, int off, int len) throws IOException;

  long skip(long len) throws IOException;

  int available() throws IOException;

  @Override
  void close() throws IOException;

  boolean markSupported();

  long transferTo(OutputStream out) throws IOException;
}
