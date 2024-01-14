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

package io.clonecloudstore.common.quarkus.example.server;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import io.clonecloudstore.common.quarkus.client.InputStreamBusinessOut;
import io.clonecloudstore.common.quarkus.example.WaitingInputStream;
import io.clonecloudstore.common.quarkus.example.client.ApiQuarkusClientFactory;
import io.clonecloudstore.common.quarkus.example.model.ApiBusinessIn;
import io.clonecloudstore.common.quarkus.example.model.ApiBusinessOut;
import io.clonecloudstore.common.quarkus.exception.CcsAlreadyExistException;
import io.clonecloudstore.common.quarkus.exception.CcsClientGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericExceptionMapper;
import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.common.quarkus.server.service.NativeStreamHandlerAbstract;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.inputstream.CipherInputStream;
import io.clonecloudstore.common.standard.inputstream.MultipleActionsInputStream;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.RequestScoped;
import org.jboss.logging.Logger;

import static io.clonecloudstore.common.quarkus.example.client.ApiConstants.X_CREATION_DATE;
import static io.clonecloudstore.common.quarkus.example.client.ApiConstants.X_LEN;
import static io.clonecloudstore.common.quarkus.example.client.ApiConstants.X_NAME;
import static io.clonecloudstore.common.quarkus.example.server.ApiQuarkusService.CIPHER;
import static io.clonecloudstore.common.quarkus.example.server.ApiQuarkusService.DELAY_TEST;
import static io.clonecloudstore.common.quarkus.example.server.ApiQuarkusService.PROXY_COMP_TEST;
import static io.clonecloudstore.common.quarkus.example.server.ApiQuarkusService.PROXY_TEST;
import static io.clonecloudstore.common.quarkus.example.server.ApiQuarkusService.THROWABLE_NAME;
import static io.clonecloudstore.common.quarkus.example.server.ApiQuarkusService.ULTRA_COMPRESSION_TEST;
import static io.clonecloudstore.common.quarkus.example.server.ApiQuarkusService.cipherDec;
import static io.clonecloudstore.common.quarkus.example.server.ApiQuarkusService.cipherEnc;

@RequestScoped
public class ServerNativeStreamHandler extends NativeStreamHandlerAbstract<ApiBusinessIn, ApiBusinessOut> {
  private static final Logger LOG = Logger.getLogger(ServerNativeStreamHandler.class);
  private final ApiQuarkusClientFactory factory;

  protected ServerNativeStreamHandler(final ApiQuarkusClientFactory factory) {
    // CDI
    this.factory = factory;
  }

  @Override
  protected boolean checkDigestToCompute(final ApiBusinessIn businessIn) {
    return QuarkusProperties.serverComputeSha256() && !businessIn.name.startsWith(PROXY_TEST);
  }

  @Override
  protected void checkPushAble(final ApiBusinessIn apiBusinessIn, final MultipleActionsInputStream inputStream)
      throws CcsClientGenericException, CcsServerGenericException {
    // Business code should come here (example: check through Object Storage that object does not exist yet, and if so
    // add the consumption of the stream for the Object Storage object creation)
    LOG.debugf("DEBUG Name %s", apiBusinessIn.name);
    switch (apiBusinessIn.name) {
      case ApiQuarkusService.CONFLICT_NAME -> {
        LOG.error("Conflict Test");
        throw new CcsAlreadyExistException("Conflict test");
      }
      case THROWABLE_NAME -> {
        LOG.error("Throw Test");
        throw new IllegalArgumentException("Incorrect name to test runtime exception");
      }
      default -> LOG.debugf("DEBUG %s no issue", apiBusinessIn.name);
    }
    LOG.debugf("DEBUG Post State %b %b %b %b", shallCompress(), shallDecompress(), isKeepInputStreamCompressed(),
        isAlreadyCompressed());
    if (apiBusinessIn.name.startsWith(PROXY_COMP_TEST)) {
      final var finalName = apiBusinessIn.name.substring(PROXY_COMP_TEST.length());
      SystemTools.STANDARD_EXECUTOR_SERVICE.execute(() -> {
        try (final var client = factory.newClient()) {
          client.setOpId(getOpId());
          LOG.debugf("DEBUG Name Post %s", apiBusinessIn.name);
          final var businessOut =
              client.postInputStreamThrough(finalName, inputStream, apiBusinessIn.len, true, isAlreadyCompressed());
          LOG.debugf("DEBUG Name Post %s (%s)", apiBusinessIn.name, businessOut);
          setResultFromRemote(businessOut);
        } catch (final CcsWithStatusException e) {
          LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
          exceptionAtomicReference.compareAndSet(null, e);
        } catch (final Exception e) {
          LOG.error("DEBUG should not be", e);
          exceptionAtomicReference.compareAndSet(null, e);
        } finally {
          endPush();
        }
      });
      Thread.yield();
    } else if (apiBusinessIn.name.startsWith(PROXY_TEST)) {
      final var finalName = apiBusinessIn.name.substring(PROXY_TEST.length());
      SystemTools.STANDARD_EXECUTOR_SERVICE.execute(() -> {
        ApiBusinessOut businessOut = null;
        try (final var client = factory.newClient()) {
          client.setOpId(getOpId());
          LOG.debugf("DEBUG Post State Through Client %b %b %b %b (%s)", shallCompress(), shallDecompress(),
              isKeepInputStreamCompressed(), isAlreadyCompressed(), getOpId());
          businessOut = client.postInputStreamThrough(finalName, inputStream, apiBusinessIn.len, shallCompress(),
              isAlreadyCompressed());
          LOG.debugf("DEBUG Name Post %s (%s)", apiBusinessIn.name, businessOut);
          setResultFromRemote(businessOut);
        } catch (final CcsWithStatusException e) {
          LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
          exceptionAtomicReference.compareAndSet(null, e);
        } catch (final Exception e) {
          LOG.error("DEBUG should not be", e);
          exceptionAtomicReference.compareAndSet(null, e);
        } finally {
          endPush();
        }
      });
      Thread.yield();
    } else {
      LOG.debugf("DEBUG Standard Consume %s", apiBusinessIn);
      SystemTools.STANDARD_EXECUTOR_SERVICE.execute(() -> {
        // Business code should come here
        try {
          final var bytes = new byte[QuarkusProperties.getBufSize()];
          if (apiBusinessIn.name.endsWith(CIPHER)) {
            final CipherInputStream cipherStream;
            try {
              cipherStream = new CipherInputStream(inputStream, cipherDec);
            } catch (IOException e) {
              exceptionAtomicReference.compareAndSet(null, e);
              return;
            }
            long time = System.currentTimeMillis();
            while (true) {
              long now = System.currentTimeMillis();
              if (now - time > StandardProperties.getMaxWaitMs()) {
                exceptionAtomicReference.compareAndSet(null, new CcsOperationException("Time out"));
                break;
              }
              time = now;
              try {
                if (cipherStream.read(bytes, 0, bytes.length) < 0) {
                  break;
                }
              } catch (final IOException e) {
                exceptionAtomicReference.compareAndSet(null, e);
                break;
              }
            }
            SystemTools.silentlyCloseNoException(cipherStream);
          } else {
            long time = System.currentTimeMillis();
            while (true) {
              long now = System.currentTimeMillis();
              if (now - time > StandardProperties.getMaxWaitMs()) {
                exceptionAtomicReference.compareAndSet(null, new CcsOperationException("Time out"));
                break;
              }
              time = now;
              try {
                if (inputStream.read(bytes, 0, bytes.length) < 0) {
                  break;
                }
              } catch (final IOException e) {
                exceptionAtomicReference.compareAndSet(null, e);
                break;
              }
            }
            LOG.infof("DEBUG: %s", inputStream);
          }
        } finally {
          endPush();
        }
      });
      Thread.yield();
    }
  }

  @Override
  protected ApiBusinessOut getAnswerPushInputStream(final ApiBusinessIn apiBusinessIn, final String finalHash,
                                                    final long size)
      throws CcsClientGenericException, CcsServerGenericException {
    // Business code should come here (example: getting the StorageObject object)
    LOG.debugf("DEBUG Answer Post (%s) %s (%d)", getOpId(), apiBusinessIn, size);
    if (apiBusinessIn.name.startsWith(PROXY_TEST)) {
      checkEndOfPush(true);
      final var responseObject = resultProxy.get();
      if (responseObject == null) {
        throwTrappedException();
        throw new CcsOperationException("Unknown result");
      }
      return responseObject;
    } else {
      checkEndOfPush(false);
    }
    if (apiBusinessIn.name.equals(THROWABLE_NAME)) {
      throwTrappedException();
      throw new CcsOperationException("Throwable test");
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
    return new HashMap<>();
  }

  @Override
  protected boolean checkPullAble(final ApiBusinessIn apiBusinessIn, final MultiMap headers)
      throws CcsClientGenericException, CcsServerGenericException {
    // Business code should come here
    // Put here business logic to check if the GET is valid (example: ObjectStorage check of existence of object)
    if (THROWABLE_NAME.equals(apiBusinessIn.name)) {
      LOG.error("Throw test");
      throw new IllegalArgumentException("Incorrect name to test runtime exception");
    }
    if (apiBusinessIn.name.equals(ApiQuarkusService.NOT_FOUND_NAME)) {
      LOG.error("Conflict Test");
    }
    return !apiBusinessIn.name.equals(ApiQuarkusService.NOT_FOUND_NAME);
  }

  @Override
  protected InputStream getPullInputStream(final ApiBusinessIn apiBusinessIn)
      throws CcsClientGenericException, CcsServerGenericException {
    // Business code should come here (example: getting the Object Storage object stream)
    LOG.debugf("Status (%s) shallComp %b shallDecomp %b", apiBusinessIn, shallCompress(), shallDecompress());
    if (apiBusinessIn.name.startsWith(PROXY_COMP_TEST)) {
      final var finalName = apiBusinessIn.name.substring(PROXY_COMP_TEST.length());
      final var client = factory.newClient();
      client.setOpId(getOpId());
      final InputStreamBusinessOut<ApiBusinessOut> inputStreamBusinessOut;
      try {
        inputStreamBusinessOut = client.getInputStream(finalName, apiBusinessIn.len, true, !shallCompress());
      } catch (final CcsWithStatusException e) {
        throw CcsServerGenericExceptionMapper.getCcsException(e.getStatus(), e.getMessage(), e);
      }
      setResultFromRemote(inputStreamBusinessOut.dtoOut());
      setKeepInputStreamCompressed(shallCompress());
      setResponseCompressed(inputStreamBusinessOut.compressed());
      return inputStreamBusinessOut.inputStream();
    } else if (apiBusinessIn.name.startsWith(PROXY_TEST)) {
      final var finalName = apiBusinessIn.name.substring(PROXY_TEST.length());
      final var client = factory.newClient();
      client.setOpId(getOpId());
      final InputStreamBusinessOut<ApiBusinessOut> inputStreamBusinessOut;
      try {
        inputStreamBusinessOut =
            client.getInputStream(finalName, apiBusinessIn.len, shallCompress(), shallDecompress());
      } catch (final CcsWithStatusException e) {
        throw CcsServerGenericExceptionMapper.getCcsException(e.getStatus(), e.getMessage(), e);
      }
      setResultFromRemote(inputStreamBusinessOut.dtoOut());
      setKeepInputStreamCompressed(shallCompress());
      setResponseCompressed(inputStreamBusinessOut.compressed());
      return inputStreamBusinessOut.inputStream();
    }
    var len = apiBusinessIn.len;
    if (len <= 0) {
      // Should come from DB for instance
      len = ApiQuarkusService.LEN;
    }
    if (apiBusinessIn.name.endsWith(CIPHER)) {
      try {
        return new CipherInputStream(new FakeInputStream(len, (byte) 'A'), cipherEnc);
      } catch (IOException e) {
        throw new CcsOperationException(e);
      }
    }
    if (apiBusinessIn.name.startsWith(DELAY_TEST)) {
      final var delay = StandardProperties.getMaxWaitMs() * 2;
      return new WaitingInputStream(len, delay);
    }
    if (apiBusinessIn.name.startsWith(ULTRA_COMPRESSION_TEST)) {
      return new FakeInputStream(len, (byte) 'A');
    }
    return new FakeInputStream(len);
  }

  @Override
  protected Map<String, String> getHeaderPullInputStream(final ApiBusinessIn apiBusinessIn)
      throws CcsClientGenericException, CcsServerGenericException {
    // Business code should come here (example: headers for object name, object size...)
    final Map<String, String> map = new HashMap<>();
    if (apiBusinessIn.name.startsWith(PROXY_TEST)) {
      final var businessOut = resultProxy.get();
      if (businessOut != null) {
        map.put(X_NAME, businessOut.name);
        map.put(X_CREATION_DATE, businessOut.creationDate.toString());
        final var len = businessOut.len;
        map.put(X_LEN, Long.toString(len));
      }
    } else {
      map.put(X_NAME, apiBusinessIn.name);
      map.put(X_CREATION_DATE, Instant.now().toString());
      var len = apiBusinessIn.len;
      if (len <= 0) {
        len = ApiQuarkusService.LEN;
      }
      map.put(X_LEN, Long.toString(len));
    }
    return map;
  }

  @Override
  protected Map<String, String> getHeaderError(final ApiBusinessIn apiBusinessIn, final int status) {
    // Business code should come here (example: get headers in case of error as Object name, Bucket name...)
    final Map<String, String> map = new HashMap<>();
    map.put(X_NAME, apiBusinessIn.name);
    return map;
  }
}
