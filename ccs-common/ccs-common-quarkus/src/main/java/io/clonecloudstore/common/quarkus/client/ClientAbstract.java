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

package io.clonecloudstore.common.quarkus.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;

import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.inputstream.MultipleActionsInputStream;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

/**
 * Client Abstraction with prepared methods for Get and Post request using InputStream with Netty
 *
 * @param <I> the type for Business Input request (in GET or POST)
 * @param <O> the type for Business Output request (in POST)
 * @param <S> the type for the Rest Service as Quarkus definition
 */
public abstract class ClientAbstract<I, O, S extends Closeable> extends SimpleClientAbstract<S> {
  private static final Logger LOGGER = Logger.getLogger(ClientAbstract.class);
  public static final int CONTEXT_SENDING = 1;
  public static final int CONTEXT_RECEIVE = -CONTEXT_SENDING;

  /**
   * Constructor used by the Factory
   */
  protected ClientAbstract(final ClientFactoryAbstract<S> factory, final URI uri) {
    super(factory, uri);
  }

  /**
   * @return the BusinessOut from the response content and/or headers
   */
  protected abstract O getApiBusinessOutFromResponse(final Response response);

  /**
   * @param context 1 for sending InputStream, -1 for receiving InputStream, or anything else
   * @return the headers map
   */
  protected abstract Map<String, String> getHeadersFor(I businessIn, int context);

  @Override
  protected ClientFactoryAbstract<S> getFactory() {
    return (ClientFactoryAbstract<S>) super.getFactory();
  }

  protected InputStream prepareInputStreamToSend(final InputStream content, final boolean shallCompress,
                                                 final boolean alreadyCompressed, final I businessIn) {
    SimpleClientAbstract.setHeadersMap(getHeadersFor(businessIn, CONTEXT_SENDING));
    SimpleClientAbstract.bodyCompressed(shallCompress || alreadyCompressed);
    MultipleActionsInputStream inputStream = MultipleActionsInputStream.create(content);
    if (shallCompress && !alreadyCompressed) {
      try {
        inputStream.compress();
      } catch (IOException e) {
        throw new CcsOperationException(e);
      }
    }
    return inputStream;
  }

  protected O getResultFromPostInputStreamUni(Uni<Response> uni, final InputStream sendInputStream)
      throws CcsWithStatusException {
    Thread.yield();
    try {
      return getApiBusinessOutFromResponse((Response) exceptionMapper.handleUniObject(this, uni, sendInputStream));
    } finally {
      resetQueryContext();
      resetMdcOpId();
    }
  }

  protected void prepareInputStreamToReceive(final boolean acceptCompressed, final I businessIn) {
    SimpleClientAbstract.setHeadersMap(getHeadersFor(businessIn, CONTEXT_RECEIVE));
    SimpleClientAbstract.acceptCompression(acceptCompressed);
  }

  protected InputStreamBusinessOut<O> getInputStreamBusinessOutFromUni(final boolean acceptCompressed,
                                                                       final boolean shallDecompress,
                                                                       final Uni<InputStream> inputStreamUni)
      throws CcsWithStatusException {
    InputStream inputStream = null;
    try {
      inputStream = (InputStream) exceptionMapper.handleUniObject(this, inputStreamUni);
      inputStream = MultipleActionsInputStream.create(inputStream);
      O businessOut = (O) SimpleClientAbstract.getDtoFromHeaders();
      LOGGER.debugf("Status (%s) acceptComp %b shallDecomp %b", businessOut, acceptCompressed, shallDecompress);
      if (acceptCompressed && shallDecompress) {
        ((MultipleActionsInputStream) inputStream).decompress();
      }
      return new InputStreamBusinessOut<>(businessOut, inputStream);
    } catch (IOException e) {
      SystemTools.consumeWhileErrorInputStream(inputStream, StandardProperties.getMaxWaitMs());
      throw new CcsOperationException(e);
    } catch (final WebApplicationException e) {
      SystemTools.consumeWhileErrorInputStream(inputStream, StandardProperties.getMaxWaitMs());
      exceptionMapper.responseToThrowable(e.getResponse());
      return null;
    } finally {
      resetQueryContext();
      resetMdcOpId();
    }
  }
}
