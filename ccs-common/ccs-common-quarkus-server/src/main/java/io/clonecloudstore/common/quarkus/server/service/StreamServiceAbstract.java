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

package io.clonecloudstore.common.quarkus.server.service;

import java.io.InputStream;

import io.clonecloudstore.common.quarkus.client.SimpleClientAbstract;
import io.clonecloudstore.common.quarkus.exception.CcsClientGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.quarkus.resteasy.reactive.server.Closer;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import static io.clonecloudstore.common.quarkus.client.SimpleClientAbstract.X_ERROR;
import static io.clonecloudstore.common.quarkus.client.SimpleClientAbstract.X_MODULE;
import static io.clonecloudstore.common.quarkus.client.SimpleClientAbstract.X_OP_ID;

/**
 * Abstraction to enable InputStream Get (Pull) and Post (Push) implementations.
 * Note that real Service must specify @Blocking on the corresponding methods with almost same
 * interface except the way business information are transmitted.
 *
 * @param <I>> the extra input information from business point of view
 * @param <O>  the output information that will be output from business point
 *             of view (if any)
 */
public abstract class StreamServiceAbstract<I, O, H extends NativeStreamHandlerAbstract<I, O>> {
  private static final Logger LOGGER = Logger.getLogger(StreamServiceAbstract.class);

  @Inject
  H nativeStream;

  protected H getNativeStream() {
    return nativeStream;
  }

  /**
   * Method to use within the POST query definition with a @Blocking annotation.
   * This method should be called by the REST API to handle the received InputStream.
   *
   * @param keepInputStreamCompressed If True, and if the InputStream is compressed, will kept as is; else will
   *                                  decompress the InputStream if it is compressed
   */
  protected Uni<Response> createObject(final HttpServerRequest request, final Closer closer, final I businessIn,
                                       final long len, final String optionalHash,
                                       final boolean keepInputStreamCompressed, final InputStream inputStream) {
    LOGGER.debugf("POST start");
    nativeStream.setup(request, closer, true, businessIn, len, optionalHash, keepInputStreamCompressed);
    return Uni.createFrom().emitter(em -> {
      try {
        final var result = nativeStream.upload(inputStream);
        em.complete(result);
      } catch (NativeServerResponseException e) {
        em.complete(e.getResponse());
      } catch (final Exception e) {
        em.complete(createErrorResponse(e));
      }
    });
  }

  /**
   * Method to use within the GET query definition with a @Blocking annotation.
   * Usually len is 0 but might be a hint on expected InputStream size.
   *
   * @param alreadyCompressed If True, and if the InputStream is to be compressed, will be kept as is; else will
   *                          compress the InputStream if it has to be
   */
  protected Uni<Response> readObject(final HttpServerRequest request, final Closer closer, final I businessIn,
                                     final boolean alreadyCompressed) {
    LOGGER.debugf("GET start");
    nativeStream.setup(request, closer, false, businessIn, 0, null, alreadyCompressed);
    return Uni.createFrom().emitter(em -> {
      try {
        em.complete(nativeStream.pull());
      } catch (NativeServerResponseException e) {
        em.complete(e.getResponse());
      } catch (final Exception e) {
        em.complete(createErrorResponse(e));
      }
    });
  }

  protected Response createErrorResponse(final Exception e) {
    if (e instanceof NativeServerResponseException ne) {
      return ne.getResponse();
    }
    final var responseBuild = switch (e) {
      case final CcsWithStatusException cse -> Response.status(cse.getStatus());
      case final CcsClientGenericException be -> Response.status(be.getStatus());
      case final CcsServerGenericException be -> Response.status(be.getStatus());
      default -> Response.serverError();
    };
    return responseBuild.header(X_ERROR, e.getMessage()).header(X_MODULE, QuarkusProperties.getCcsModule().name())
        .header(X_OP_ID, SimpleClientAbstract.getMdcOpId())
        .header(HttpHeaderNames.CONNECTION.toString(), HttpHeaderValues.CLOSE.toString()).build();
  }
}