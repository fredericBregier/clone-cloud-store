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
import java.util.Map;

import io.clonecloudstore.common.quarkus.client.SimpleClientAbstract;
import io.clonecloudstore.common.quarkus.exception.CcsClientGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericExceptionMapper;
import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.common.standard.inputstream.DigestAlgo;
import io.clonecloudstore.common.standard.inputstream.MultipleActionsInputStream;
import io.clonecloudstore.common.standard.inputstream.ZstdCompressInputStream;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.quarkus.resteasy.reactive.server.Closer;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.mutiny.core.Vertx;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import static io.clonecloudstore.common.quarkus.client.SimpleClientAbstract.X_OP_ID;

@Dependent
public abstract class NativeStreamHandlerAbstract<I, O> {
  private static final Logger LOG = Logger.getLogger(NativeStreamHandlerAbstract.class);
  private HttpServerRequest request;
  private boolean keepAlive;
  //@Inject
  Vertx vertx;
  private String opId;
  private boolean shallCompress;
  private boolean alreadyCompressed;
  private Closer closer;
  private MultipleActionsInputStream waitForAllReadInputStream = null;
  private I businessIn;
  private boolean isUpload;
  private boolean keepInputStreamCompressed;
  private String originalHash;
  private long inputStreamLength;
  private boolean shallDecompress;

  protected NativeStreamHandlerAbstract() {
    // CDI
    vertx = CDI.current().select(Vertx.class).get();
  }

  /**
   * Constructor, while inputLength could be 0 (valid for both POST and GET, signifying the posted
   * InputStream length or the supposed returned InputStream length).
   * When the stream is incoming (server side, so using POST, stream coming from client), if compressed, the
   * configuration shall decide if the stream is kept compressed or decompressed before passing the stream to its
   * final usage.
   *
   * @param inputLength               might be 0 if unknown size
   * @param optionalHash              Hash optional (might be null); if POST InputStream, if empty, Hash will be
   *                                  computed
   * @param keepInputStreamCompressed True means the stream will be kept compressed if source (from client) is
   *                                  compressed or do not recompress getInputStream
   * @param isUpload                  True for incoming InputStream, False for outgoing InputStream
   */
  public void setup(final HttpServerRequest request, final Closer closer, final boolean isUpload, final I businessIn,
                    final long inputLength, final String optionalHash, final boolean keepInputStreamCompressed) {
    this.request = request;
    this.closer = closer;
    final var multiMap = request.headers();
    keepAlive = !HttpHeaderValues.CLOSE.toString().equals(multiMap.get(HttpHeaderNames.CONNECTION));
    var len = 0L;
    final var length = multiMap.get(HttpHeaderNames.CONTENT_LENGTH);
    if (ParametersChecker.isNotEmpty(length)) {
      len = Long.parseLong(length);
    }
    inputStreamLength = len;
    shallCompress = multiMap.contains(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.ZSTD, true);
    alreadyCompressed = multiMap.contains(HttpHeaderNames.CONTENT_ENCODING, HttpHeaderValues.ZSTD, true);
    var opIdHeader = multiMap.get(X_OP_ID);
    if (opIdHeader == null) {
      opIdHeader = GuidLike.getGuid();
    }
    opId = opIdHeader;
    SimpleClientAbstract.setMdcOpId(opId);

    this.businessIn = businessIn;
    this.isUpload = isUpload;
    this.keepInputStreamCompressed = keepInputStreamCompressed;
    originalHash = optionalHash;
    if (inputLength > 0) {
      inputStreamLength = inputLength;
    }
    if (isUpload) {
      shallDecompress = !keepInputStreamCompressed && alreadyCompressed;
    } else {
      // Never in read?
      shallDecompress = false;
    }
  }

  public Response pull() throws NativeServerResponseException {
    try {
      preparePull();
      // Proxy operation is possible, so getting first InputStream in order to get correct Headers then
      return doGetInputStream();
    } catch (final CcsClientGenericException | CcsServerGenericException e) {
      // Change to create an exception with response that will be used in case of error
      sendError(e.getStatus(), e);
    } catch (final NativeServerResponseException e) {
      throw e;
    } catch (final Exception e) {
      sendError(Response.Status.INTERNAL_SERVER_ERROR, e);
    } finally {
      clear();
    }
    throw getNativeClientResponseException(Response.Status.INTERNAL_SERVER_ERROR);
  }

  public Response upload(final InputStream inputStream) throws NativeServerResponseException {
    try {
      closer.add(inputStream);
      LOG.debugf("DEBUG START UPLOAD %s", businessIn);
      final var inputStreamFinal = prepareUpload(inputStream);
      if (ParametersChecker.isEmpty(getOriginalHash()) && QuarkusProperties.serverComputeSha256() &&
          checkDigestToCumpute(businessIn)) {
        inputStreamFinal.computeDigest(DigestAlgo.SHA256);
      }
      getCloser().add(inputStreamFinal);
      waitForAllReadInputStream = inputStreamFinal;
      checkPushAble(getBusinessIn(), getInputStreamLength(), waitForAllReadInputStream);
      Thread.yield();
      LOG.debugf("DEBUG Wait For All Read %s", businessIn);
      var len = waitForAllReadInputStream.waitForAllRead(QuarkusProperties.clientResponseTimeOut());
      if (len > 0) {
        inputStreamLength = len;
      } else {
        throw new CcsOperationException("Time out during upload");
      }
      LOG.infof("DEBUG %s", inputStreamFinal);
      LOG.debugf("DEBUG Wait For All Read Ended: %d", inputStreamLength);
      return getResponseUpload(inputStreamLength);
    } catch (final CcsClientGenericException | CcsServerGenericException e) {
      sendError(e.getStatus(), e);
    } catch (final NativeServerResponseException e) {
      throw e;
    } catch (final Exception e) {
      sendError(Response.Status.INTERNAL_SERVER_ERROR, e);
    } finally {
      clear();
    }
    throw getNativeClientResponseException(Response.Status.INTERNAL_SERVER_ERROR);
  }

  private void internalError(final Throwable cause) {
    if (cause instanceof CcsClientGenericException || cause instanceof CcsServerGenericException ||
        cause instanceof CcsWithStatusException || cause instanceof NativeServerResponseException) {
      LOG.info("Exception? " + cause.getMessage());
    } else {
      LOG.error("Exception? ", cause);
    }
  }

  private Response doGetInputStream() {
    var inputStream = getPullInputStream(getBusinessIn());
    closer.add(inputStream);
    final var map = getHeaderPullInputStream(getBusinessIn());
    final var response = Response.ok();
    map.put(HttpHeaderNames.TRANSFER_ENCODING.toString(), HttpHeaderValues.CHUNKED.toString());
    if (!isKeepAlive()) {
      map.put(HttpHeaderNames.CONNECTION.toString(), HttpHeaderValues.CLOSE.toString());
    }
    if (shallCompress()) {
      map.put(HttpHeaderNames.CONTENT_ENCODING.toString(), HttpHeaderValues.ZSTD.toString());
    }
    for (final var entry : map.entrySet()) {
      response.header(entry.getKey(), entry.getValue());
    }
    if (shallCompress() && !isKeepInputStreamCompressed()) {
      try {
        inputStream = new ZstdCompressInputStream(inputStream);
      } catch (final IOException e) {
        throw new CcsOperationException(e);
      }
      closer.add(inputStream);
    }
    response.entity(inputStream);
    return response.build();
  }

  private void preparePull() throws NativeServerResponseException {
    if (!checkPullAble(getBusinessIn(), getRequest().headers())) {
      var exception = new CcsNotExistException("Item not found");
      sendError(Response.Status.NOT_FOUND, exception);
    }
  }

  private Response getResponseUpload(final long len) {
    var finalHash =
        (waitForAllReadInputStream.isDigestEnabled()) ? waitForAllReadInputStream.getDigest() : getOriginalHash();
    final var businessOut = getAnswerPushInputStream(getBusinessIn(), finalHash, len);
    final var map = getHeaderPushInputStream(getBusinessIn(), finalHash, len, businessOut);
    map.put(HttpHeaderNames.CONTENT_TYPE.toString(), MediaType.APPLICATION_JSON);
    if (!isKeepAlive()) {
      map.put(HttpHeaderNames.CONNECTION.toString(), HttpHeaderValues.CLOSE.toString());
    }
    var responseBuilder = Response.status(Response.Status.CREATED);
    for (final var entry : map.entrySet()) {
      responseBuilder.header(entry.getKey(), entry.getValue());
    }
    if (businessOut != null) {
      responseBuilder.entity(businessOut);
    }
    return responseBuilder.build();
  }

  private MultipleActionsInputStream prepareUpload(final InputStream inputStream) throws NativeServerResponseException {
    try {
      MultipleActionsInputStream newInputStream = MultipleActionsInputStream.create(inputStream);
      if (shallDecompress()) {
        // Decompress before computing digest if any
        newInputStream.decompress();
      }
      return newInputStream;
    } catch (final CcsClientGenericException | CcsServerGenericException e) {
      sendError(e.getStatus(), e);
    } catch (final Exception e) {
      sendError(Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    throw new CcsOperationException("Unknown upload status");
  }

  private void sendError(final int status, final Exception cause) throws NativeServerResponseException {
    internalError(cause);
    sendErrorInternal(Response.Status.fromStatusCode(status));
  }

  private void sendError(final Response.Status status, final Throwable t) throws NativeServerResponseException {
    internalError(t);
    sendErrorInternal(status);
  }

  private void sendErrorInternal(final Response.Status status) throws NativeServerResponseException {
    if (isUpload() && waitForAllReadInputStream != null) {
      SystemTools.consumeWhileErrorInputStream(waitForAllReadInputStream, StandardProperties.getMaxWaitMs());
    }
    final var exception = getNativeClientResponseException(status);
    LOG.debug(status, exception);
    throw exception;
  }

  private NativeServerResponseException getNativeClientResponseException(final Response.Status status) {
    final var map = getHeaderError(getBusinessIn(), status.getStatusCode());
    //map.put(HttpHeaderNames.CONNECTION.toString(), HttpHeaderValues.CLOSE.toString());
    final var responseBuilder = Response.status(status);
    for (final var entry : map.entrySet()) {
      responseBuilder.header(entry.getKey(), entry.getValue());
    }
    return new NativeServerResponseException(responseBuilder.build(),
        CcsServerGenericExceptionMapper.getCcsException(status.getStatusCode()));
  }

  /**
   * Clear all context
   */
  protected void clear() {
    // Nothing yet
  }

  /**
   * @return True if Write (client sends an InputStream), False if Read (client receives an InputStream)
   */
  protected boolean isUpload() {
    return isUpload;
  }

  /**
   * @return the Operation Id (unique)
   */
  protected String getOpId() {
    return opId;
  }

  /**
   * @return True if the InputStream is already compressed (POST)
   */
  protected boolean isAlreadyCompressed() {
    return alreadyCompressed;
  }

  /**
   * @return True if the InputStream shall be compressed (GET)
   */
  protected boolean shallCompress() {
    return shallCompress;
  }

  /**
   * @return True if the InputStream shall be decompressed (GET and POST)
   */
  protected boolean shallDecompress() {
    return shallDecompress;
  }

  /**
   * @return the passed BusinessIn during construction
   */
  protected I getBusinessIn() {
    return businessIn;
  }

  public HttpServerRequest getRequest() {
    return request;
  }

  public boolean isKeepAlive() {
    return keepAlive;
  }

  public boolean isKeepInputStreamCompressed() {
    return keepInputStreamCompressed;
  }

  public String getOriginalHash() {
    return originalHash;
  }

  public Vertx getVertx() {
    return vertx;
  }

  public long getInputStreamLength() {
    return inputStreamLength;
  }

  protected Closer getCloser() {
    return closer;
  }

  /**
   * @return True if the digest is to be computed on the fly
   */
  protected abstract boolean checkDigestToCumpute(I businessIn);

  /**
   * Check if the request for POST is valid, and if so, adapt the given NettyToInputStream that will
   * be used to consume the original InputStream.
   * The implementation shall use the business logic to check the validity for this InputStream reception
   * (from client to server) and, if valid, use the NettyToInputStream, either as is or as a standard InputStream.
   * (example: check through Object Storage that object does not exist yet, and if so
   * add the consumption of the stream for the Object Storage object creation).
   * Note that the stream might be kept compressed if keepInputStreamCompressed was specified at construction.
   *
   * @param len is the length passed at construction, so might be 0
   */
  protected abstract void checkPushAble(I businessIn, long len, MultipleActionsInputStream inputStream)
      throws CcsClientGenericException, CcsServerGenericException;

  /**
   * Returns a BusinessOut in case of POST (receiving InputStream on server side).
   * The implementation shall use the business logic to get the right
   * BusinessOut object to return.
   * (example: getting the StorageObject object, including the computed or given Hash)
   *
   * @param businessIn businessIn as passed in constructor
   * @param finalHash  the final Hash if computed on the fly, or the original given one
   * @param size       the real size read (from received stream, could be compressed size if decompression is off at
   *                   construction)
   */
  protected abstract O getAnswerPushInputStream(I businessIn, String finalHash, long size)
      throws CcsClientGenericException, CcsServerGenericException;

  /**
   * Returns a Map for Headers response in case of POST (receiving InputStream on server side).
   * (example: headers for object name, object size, ...)
   *
   * @param businessIn  businessIn as passed in constructor
   * @param finalHash   the final Hash if computed on the fly, or the original given one
   * @param size        the real size read
   * @param businessOut previously constructed from getAnswerPushInputStream
   */
  protected abstract Map<String, String> getHeaderPushInputStream(I businessIn, String finalHash, long size,
                                                                  O businessOut)
      throws CcsClientGenericException, CcsServerGenericException;

  /**
   * The implementation must check using business object that get inputStream request (server sending InputStream as
   * result) is valid according to the businessIn from te Rest API and the headers.
   * (example: ObjectStorage check of existence of object)
   *
   * @return True if the read action is valid for this businessIn object and headers
   */
  protected abstract boolean checkPullAble(I businessIn, MultiMap headers)
      throws CcsClientGenericException, CcsServerGenericException;

  /**
   * Returns the InputStream required for GET (server is sending the InputStream back to the client).
   * The implementation shall use the business logic and controls to get the InputStream to return.
   * (example: getting the Object Storage object stream)
   *
   * @param businessIn businessIn as passed in constructor
   */
  protected abstract InputStream getPullInputStream(I businessIn)
      throws CcsClientGenericException, CcsServerGenericException;

  /**
   * Returns a Map for Headers response in case of GET, added to InputStream get above  (server is sending the
   * InputStream back to the client)
   * (example: headers for object name, object size...)
   *
   * @param businessIn businessIn as passed in constructor
   */
  protected abstract Map<String, String> getHeaderPullInputStream(I businessIn)
      throws CcsClientGenericException, CcsServerGenericException;

  /**
   * Return headers for error message.
   * (example: get headers in case of error as Object name, Bucket name...)
   */
  protected abstract Map<String, String> getHeaderError(I businessIn, int status);
}
