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

package io.clonecloudstore.test.server.service;

import java.io.IOException;
import java.io.InputStream;

import io.clonecloudstore.common.quarkus.exception.CcsClientGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.common.quarkus.server.service.StreamHandlerAbstract;
import io.clonecloudstore.common.standard.inputstream.MultipleActionsInputStream;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.test.stream.FakeInputStream;
import jakarta.enterprise.context.Dependent;
import org.jboss.logging.Logger;

@Dependent
public abstract class FakeStreamHandlerAbstract<I, O> extends StreamHandlerAbstract<I, O> {
  private static final Logger LOGGER = Logger.getLogger(FakeStreamHandlerAbstract.class);
  private static final long LEN = 100 * 1024 * 1024L;

  @Override
  protected void checkPushAble(final I apiBusinessIn, final MultipleActionsInputStream inputStream)
      throws CcsClientGenericException, CcsServerGenericException {
    // Business code should come here (example: check through Object Storage that object does not exist yet, and if so
    // add the consumption of the stream for the Object Storage object creation)
    LOGGER.debugf("Post State %b %b %b %b", shallCompress(), shallDecompress(), isKeepInputStreamCompressed(),
        isAlreadyCompressed());
    LOGGER.debugf("Standard Consume %s", apiBusinessIn);
    SystemTools.STANDARD_EXECUTOR_SERVICE.execute(() -> {
      // Business code should come here
      try {
        final var bytes = new byte[StandardProperties.getBufSize()];
        long time = System.currentTimeMillis();
        var still = true;
        while (still) {
          long now = System.currentTimeMillis();
          if (now - time > StandardProperties.getMaxWaitMs()) {
            exceptionAtomicReference.compareAndSet(null, new CcsOperationException("Time out"));
            break;
          }
          time = now;
          try {
            if (inputStream.read(bytes, 0, bytes.length) < 0) {
              still = false;
            }
          } catch (final IOException e) {
            exceptionAtomicReference.compareAndSet(null, e);
            still = false;
          }
        }
      } finally {
        endPush();
      }
    });
    Thread.yield();
  }

  protected abstract O getBusinessOutForPushAnswer(final I apiBusinessIn, final String finalHash, final long size);

  @Override
  protected O getAnswerPushInputStream(final I apiBusinessIn, final String finalHash, final long size)
      throws CcsClientGenericException, CcsServerGenericException {
    // Business code should come here (example: getting the StorageObject object)
    LOGGER.debugf("Answer Post (%s) %s (%d)", getOpId(), apiBusinessIn, size);
    checkEndOfPush(false);
    return getBusinessOutForPushAnswer(apiBusinessIn, finalHash, size);
  }

  protected abstract long getLengthFromBusinessIn(I businessIn);

  @Override
  protected InputStream getPullInputStream(final I apiBusinessIn)
      throws CcsClientGenericException, CcsServerGenericException {
    // Business code should come here (example: getting the Object Storage object stream)
    LOGGER.debugf("Status (%s) shallComp %b shallDecomp %b", apiBusinessIn, shallCompress(), shallDecompress());
    var len = getLengthFromBusinessIn(apiBusinessIn);
    if (len <= 0) {
      // Should come from DB for instance
      len = LEN;
    }
    return new FakeInputStream(len, (byte) 'A');
  }
}
