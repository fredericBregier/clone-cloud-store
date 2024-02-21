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

package io.clonecloudstore.common.standard.inputstream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.clonecloudstore.common.standard.system.SystemTools;

import static io.clonecloudstore.common.standard.system.SystemTools.VIRTUAL_EXECUTOR_SERVICE;

public class TransferInputStream extends AbstractCommonInputStream {
  private static final Logger LOGGER = Logger.getLogger(TransferInputStream.class.getName());
  private final AtomicReference<Exception> exceptionAtomicReference;

  @Override
  protected OutputStream getNewOutputStream(final Object extraArgument) {
    return null;
  }

  public TransferInputStream(final InputStream inputStream, final AtomicReference<Exception> exceptionAtomicReference)
      throws IOException {
    super(inputStream, null);
    this.exceptionAtomicReference =
        exceptionAtomicReference != null ? exceptionAtomicReference : new AtomicReference<>();
  }

  public void startCopyAsync() {
    SystemTools.STANDARD_EXECUTOR_SERVICE.execute(this::transferFromSource);
    Thread.yield();
  }

  private void transferFromSource() {
    try {
      sizeRead += SystemTools.transferTo(this.inputStream, this.pipedOutputStream);
    } catch (final IOException e) {
      LOGGER.log(Level.SEVERE, e.getMessage());
      exceptionAtomicReference.compareAndSet(null, e);
      ioExceptionAtomicReference.compareAndSet(null, e);
      Thread.yield();
    } finally {
      VIRTUAL_EXECUTOR_SERVICE.execute(() -> {
        SystemTools.silentlyCloseNoException(pipedOutputStream);
        SystemTools.silentlyCloseNoException(this.inputStream);
      });
      Thread.yield();
    }
  }

  @Override
  protected void checkException() throws IOException {
    if (exceptionAtomicReference.get() != null) {
      final var exc = exceptionAtomicReference.get();
      if (exc instanceof IOException e) {
        throw e;
      }
      throw new IOException(exc);
    }
    if (ioExceptionAtomicReference.get() != null) {
      throw ioExceptionAtomicReference.get();
    }
  }
}
