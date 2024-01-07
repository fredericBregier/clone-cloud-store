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

package io.clonecloudstore.common.quarkus.client.example.server;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import io.clonecloudstore.common.quarkus.client.example.ApiConstants;
import io.clonecloudstore.common.quarkus.client.example.model.ApiBusinessIn;
import io.clonecloudstore.common.quarkus.client.example.model.ApiBusinessOut;
import io.clonecloudstore.common.quarkus.exception.CcsClientGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.common.standard.inputstream.MultipleActionsInputStream;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.RequestScoped;
import org.jboss.logging.Logger;

import static io.clonecloudstore.common.quarkus.client.example.ApiConstants.X_NAME;
import static io.clonecloudstore.common.standard.properties.StandardProperties.STANDARD_EXECUTOR_SERVICE;

@RequestScoped
public class ServerNativeStreamHandler extends NativeStreamHandlerAbstract<ApiBusinessIn, ApiBusinessOut> {
  private static final Logger LOG = Logger.getLogger(ServerNativeStreamHandler.class);
  private final Semaphore semaphore = new Semaphore(0);

  protected ServerNativeStreamHandler() {
  }

  @Override
  protected void clear() {
    semaphore.release();
    super.clear();
  }

  @Override
  protected boolean checkDigestToCumpute(final ApiBusinessIn businessIn) {
    return true;
  }

  @Override
  protected void checkPushAble(final ApiBusinessIn apiBusinessIn, final long len,
                               final MultipleActionsInputStream inputStream)
      throws CcsClientGenericException, CcsServerGenericException {
    // Business code should come here (example: check through Object Storage that object does not exist yet, and if so
    // add the consumption of the stream for the Object Storage object creation)
    STANDARD_EXECUTOR_SERVICE.execute(() -> {
      try {
        var length = FakeInputStream.consumeAll(inputStream);
        LOG.infof("POST Size: %d", length);
      } catch (IOException e) {
        LOG.error(e, e);
      } finally {
        semaphore.release();
      }
    });
    Thread.yield();
  }

  @Override
  protected ApiBusinessOut getAnswerPushInputStream(final ApiBusinessIn apiBusinessIn, final String finalHash,
                                                    final long size)
      throws CcsClientGenericException, CcsServerGenericException {
    // Business code should come here (example: getting the StorageObject object)
    LOG.debugf("DEBUG Answer Post (%s) %s (%d)", getOpId(), apiBusinessIn, size);
    try {
      if (!semaphore.tryAcquire(StandardProperties.getMaxWaitMs(), TimeUnit.MILLISECONDS)) {
        throw new CcsOperationException("Time Out");
      }
    } catch (InterruptedException e) {
      throw new CcsOperationException(e);
    }
    final var businessOut = new ApiBusinessOut();
    businessOut.name = apiBusinessIn.name;
    businessOut.len = size;
    businessOut.creationDate = Instant.now();
    return businessOut;
  }

  @Override
  protected Map<String, String> getHeaderPushInputStream(final ApiBusinessIn apiBusinessIn, final String finalHash,
                                                         final long size, final ApiBusinessOut apiBusinessOut)
      throws CcsClientGenericException, CcsServerGenericException {
    // Business code should come here (example: headers for object name, object size, ...)
    return getHeaderMap(apiBusinessOut);
  }

  @Override
  protected boolean checkPullAble(final ApiBusinessIn apiBusinessIn, final MultiMap headers)
      throws CcsClientGenericException, CcsServerGenericException {
    // Business code should come here
    // Put here business logic to check if the GET is valid (example: ObjectStorage check of existence of object)
    return true;
  }

  @Override
  protected InputStream getPullInputStream(final ApiBusinessIn apiBusinessIn)
      throws CcsClientGenericException, CcsServerGenericException {
    // Business code should come here (example: getting the Object Storage object stream)
    final var len =
        apiBusinessIn.len > 0 ? apiBusinessIn.len : getInputStreamLength() > 0 ? getInputStreamLength() : 1000000;
    return new FakeInputStream(len, (byte) 'A');
  }

  @Override
  protected Map<String, String> getHeaderPullInputStream(final ApiBusinessIn apiBusinessIn)
      throws CcsClientGenericException, CcsServerGenericException {
    // Business code should come here (example: headers for object name, object size...)
    return getHeaderMap(apiBusinessIn);
  }

  @Override
  protected Map<String, String> getHeaderError(final ApiBusinessIn apiBusinessIn, final int status) {
    // Business code should come here (example: get headers in case of error as Object name, Bucket name...)
    final Map<String, String> map = new HashMap<>();
    map.put(X_NAME, apiBusinessIn.name);
    return map;
  }

  private static Map<String, String> getHeaderMap(final ApiBusinessOut apiBusinessOut) {
    final var map = new HashMap<String, String>();
    map.put(X_NAME, apiBusinessOut.name);
    map.put(ApiConstants.X_LEN, Long.toString(apiBusinessOut.len));
    map.put(ApiConstants.X_CREATION_DATE,
        apiBusinessOut.creationDate != null ? apiBusinessOut.creationDate.toString() : "");
    return map;
  }

  private static Map<String, String> getHeaderMap(final ApiBusinessIn apiBusinessIn) {
    final var map = new HashMap<String, String>();
    map.put(X_NAME, apiBusinessIn.name);
    map.put(ApiConstants.X_LEN, Long.toString(apiBusinessIn.len));
    map.put(ApiConstants.X_CREATION_DATE, Instant.now().toString());
    return map;
  }

}
