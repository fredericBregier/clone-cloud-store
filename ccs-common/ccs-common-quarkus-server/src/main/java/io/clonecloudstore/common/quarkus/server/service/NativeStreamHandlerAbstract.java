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

package io.clonecloudstore.common.quarkus.server.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
import io.quarkus.resteasy.reactive.server.Closer;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.mutiny.core.Vertx;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import static io.clonecloudstore.common.standard.properties.ApiConstants.CHUNKED;
import static io.clonecloudstore.common.standard.properties.ApiConstants.CLOSE;
import static io.clonecloudstore.common.standard.properties.ApiConstants.COMPRESSION_ZSTD;
import static io.clonecloudstore.common.standard.properties.ApiConstants.CONNECTION;
import static io.clonecloudstore.common.standard.properties.ApiConstants.TRANSFER_ENCODING;
import static io.clonecloudstore.common.standard.properties.ApiConstants.X_OP_ID;

@Dependent
public abstract class NativeStreamHandlerAbstract<I, O> {
  private static final Logger LOGGER = Logger.getLogger(NativeStreamHandlerAbstract.class);
  private HttpServerRequest request;
  private boolean keepAlive;
  @Inject
  Vertx vertx;
  private String opId;
  private boolean shallCompress;
  private boolean alreadyCompressed;
  private Closer closer;
  private MultipleActionsInputStream waitForAllReadInputStream = null;
  private I businessIn;
  private boolean isUpload;
  private boolean keepInputStreamCompressed;
  private boolean responseCompressed;
  private String originalHash;
  private long inputStreamLength;
  private boolean shallDecompress;
  protected final AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<>();
  protected final CountDownLatch countDownLatch = new CountDownLatch(1);
  protected final AtomicReference<O> resultProxy = new AtomicReference<>();

  protected NativeStreamHandlerAbstract() {
  }

  /**
   * Method to override for post setup
   */
  protected void postSetup() {
    // Empty
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
   *                                  compressed (for push only)
   * @param isUpload                  True for incoming InputStream, False for outgoing InputStream
   */
  public void setup(final HttpServerRequest request, final Closer closer, final boolean isUpload, final I businessIn,
                    final long inputLength, final String optionalHash, final boolean keepInputStreamCompressed) {
    this.request = request;
    this.closer = closer;
    final var multiMap = request.headers();
    keepAlive = !CLOSE.equals(multiMap.get(CONNECTION));
    var len = 0L;
    final var length = multiMap.get(HttpHeaders.CONTENT_LENGTH);
    if (ParametersChecker.isNotEmpty(length)) {
      len = Long.parseLong(length);
    }
    inputStreamLength = len;
    shallCompress = multiMap.contains(HttpHeaders.ACCEPT_ENCODING, COMPRESSION_ZSTD, true);
    alreadyCompressed = multiMap.contains(HttpHeaders.CONTENT_ENCODING, COMPRESSION_ZSTD, true);
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
    postSetup();
  }

  public Response pullList() throws NativeServerResponseException {
    throw new UnsupportedOperationException("Shall be implemented");
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
      LOGGER.debugf("START UPLOAD %s", businessIn);
      final var inputStreamFinal = prepareUpload(inputStream);
      if (ParametersChecker.isEmpty(getOriginalHash()) && checkDigestToCompute(businessIn)) {
        inputStreamFinal.computeDigest(DigestAlgo.SHA256);
      }
      getCloser().add(inputStreamFinal);
      waitForAllReadInputStream = inputStreamFinal;
      checkPushAble(getBusinessIn(), waitForAllReadInputStream);
      Thread.yield();
      LOGGER.debugf("Wait For All Read %s", businessIn);
      var len = waitForAllReadInputStream.waitForAllRead(QuarkusProperties.clientResponseTimeOut());
      if (len > 0) {
        inputStreamLength = len;
      } else {
        throw new CcsOperationException("Time out during upload");
      }
      LOGGER.debugf("Wait For All Read Ended: %d", inputStreamLength);
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
      LOGGER.info("Exception? " + cause.getMessage());
    } else {
      LOGGER.error("Exception? ", cause);
    }
  }

  protected Response doGetInputStream() {
    var inputStream = getPullInputStream(getBusinessIn());
    closer.add(inputStream);
    final var map = getHeaderPullInputStream(getBusinessIn());
    final var response = Response.ok();
    map.put(TRANSFER_ENCODING, CHUNKED);
    if (!isKeepAlive()) {
      map.put(CONNECTION, CLOSE);
    }
    if (shallCompress() || (!shallCompress() && isResponseCompressed())) {
      map.put(HttpHeaders.CONTENT_ENCODING, COMPRESSION_ZSTD);
    }
    for (final var entry : map.entrySet()) {
      response.header(entry.getKey(), entry.getValue());
    }
    LOGGER.debugf("Status (%s) shallCompress %b responseCompressed %b", businessIn, shallCompress(),
        isResponseCompressed());
    if (shallCompress() && !isResponseCompressed()) {
      LOGGER.debugf("Status compress %s", businessIn);
      if (inputStream instanceof MultipleActionsInputStream mai) {
        try {
          mai.compress();
        } catch (final IOException e) {
          throw new CcsOperationException(e);
        }
      } else {
        try {
          inputStream = new ZstdCompressInputStream(inputStream);
          closer.add(inputStream);
        } catch (final IOException e) {
          throw new CcsOperationException(e);
        }
      }
    }
    response.entity(inputStream);
    return response.build();
  }

  protected void preparePull() throws NativeServerResponseException {
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
    map.put(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
    if (!isKeepAlive()) {
      map.put(CONNECTION, CLOSE);
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
      LOGGER.debugf("Status: shallComp %b shallDecomp %b", shallCompress, shallDecompress);
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

  protected void sendError(final int status, final Exception cause) throws NativeServerResponseException {
    internalError(cause);
    sendErrorInternal(Response.Status.fromStatusCode(status));
  }

  protected void sendError(final Response.Status status, final Throwable t) throws NativeServerResponseException {
    internalError(t);
    sendErrorInternal(status);
  }

  private void sendErrorInternal(final Response.Status status) throws NativeServerResponseException {
    if (isUpload() && waitForAllReadInputStream != null) {
      SystemTools.consumeWhileErrorInputStream(waitForAllReadInputStream, StandardProperties.getMaxWaitMs());
    }
    final var exception = getNativeClientResponseException(status);
    LOGGER.debug(status, exception);
    throw exception;
  }

  protected NativeServerResponseException getNativeClientResponseException(final Response.Status status) {
    final var map = getHeaderError(getBusinessIn(), status.getStatusCode());
    map.put(CONNECTION, CLOSE);
    setResultFromRemote(null);
    endPush();
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
    countDownLatch.countDown();
  }

  protected void setResultFromRemote(final O businessOut) {
    resultProxy.set(businessOut);
  }

  protected void endPush() {
    countDownLatch.countDown();
  }

  protected void checkEndOfPush(final boolean remote) {
    try {
      if (!countDownLatch.await(QuarkusProperties.clientResponseTimeOut(), TimeUnit.MILLISECONDS)) {
        if (remote) {
          throw new CcsOperationException("Time Out during Remote Write");
        }
        throw new CcsOperationException("Time Out during Local Write");
      }
      throwTrappedException();
    } catch (final InterruptedException e) {
      throw new CcsOperationException(e);
    }
  }

  protected void throwTrappedException() {
    final var exc = exceptionAtomicReference.get();
    LOGGER.debugf("Answer Post Through Error?: %s", exc);
    if (exc != null) {
      LOGGER.debug("Get exception from POST THROUGH", exc);
      switch (exc) {
        case final CcsWithStatusException cse ->
            throw CcsServerGenericExceptionMapper.getCcsException(cse.getStatus(), cse.getMessage(), cse);
        case final CcsClientGenericException be -> throw be;
        case final CcsServerGenericException be -> throw be;
        case final NativeServerResponseException ne -> {
          final var sub = ne.getException();
          if (sub instanceof CcsClientGenericException sub2) {
            throw sub2;
          } else {
            throw (CcsServerGenericException) sub;
          }
        }
        default -> throw new CcsOperationException(exc);
      }
    }
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
  public boolean shallCompress() {
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

  /**
   * True means the stream will be kept compressed if source (from client) is
   * compressed or do not recompress getInputStream
   */
  public boolean isKeepInputStreamCompressed() {
    return keepInputStreamCompressed;
  }

  public void setKeepInputStreamCompressed(final boolean change) {
    keepInputStreamCompressed = change;
  }

  /**
   * True means the incoming stream is compressed (ZSTD)
   */
  public boolean isResponseCompressed() {
    return responseCompressed;
  }

  public void setResponseCompressed(final boolean change) {
    responseCompressed = change;
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
   * Default based on QuarkusProperties.serverComputeSha256
   *
   * @return True if the digest is to be computed on the fly
   */
  protected boolean checkDigestToCompute(I businessIn) { // NOSONAR intentional argument
    return QuarkusProperties.serverComputeSha256();
  }

  /**
   * Check if the request for POST is valid, and if so, adapt the given MultipleActionsInputStream that will
   * be used to consume the original InputStream.
   * The implementation shall use the business logic to check the validity for this InputStream reception
   * (from client to server) and, if valid, use the MultipleActionsInputStream, either as is or as a standard
   * InputStream.
   * (example: check through Object Storage that object does not exist yet, and if so
   * add the consumption of the stream for the Object Storage object creation).
   * Note that the stream might be kept compressed if keepInputStreamCompressed was specified at construction.
   */
  protected abstract void checkPushAble(I businessIn, MultipleActionsInputStream inputStream)
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
