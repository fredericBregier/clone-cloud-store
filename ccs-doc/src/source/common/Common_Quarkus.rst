Common Quarkus
###############

This module contains some class to help handling InputStream within Quarkus efficiently.

Using *Uni* was not possible for InputStream since Quarkus does not support yet correctly InputStream.
A patch is submitted to enable this (see https://github.com/quarkusio/quarkus/pull/37308)
"@Blocking" mode must be declared imperatively, which means that a new thread is used.

Two cases occur:

- Sending an InputStream to a remote REST API
- Receiving an InputStream from a remote REST API


Client and Server Abstract implementation for InputStream
****************************************************************

In order to make it easier to integrate the InputStream management with back-pressure in all APIs,
an abstract implementation is provide both for Client ans Server.

The full example is located in the test part of the **ccs-common-quarkus-server**.

- ``io.clonecloudstore.common.quarkus.example.model`` contains the definition of the model of data (In and Out).

- ``io.clonecloudstore.common.quarkus.example.client`` contains the **ApiClient** and its factory and the extension of different abstract needed for the client.

The abstract **ClientAbstract** defines some abstract methods that must be specified within the final client implementation,
in order to include business implementation.

Client sending InputStream
=================================

Note that if several API are intended for this client to send InputStream (various usages), one shall specialized the answer
of those abstract methods through more general BusinessIn and BusinessOut types (for instance, using multiple sub elements or
using instanceOf check).


.. code-block:: java
  :caption: Zoom on **ClientAbstract** POST way (sending InputStream to server)

    /**
     * @param context 1 for sending InputStream, -1 for receiving InputStream, or anything else
     * @return the headers map
     */
    protected abstract Map<String, String> getHeadersFor(I businessIn, int context);

    /**
     * @return the BusinessOut from the response content and/or headers
     */
    protected abstract O getApiBusinessOutFromResponse(final Response response);


Client receiving InputStream
=================================

Note that if several API are intended for this client to receive InputStream (various usages), one shall specialized the answer
of those abstract methods through more general BusinessIn and BusinessOut types (for instance, using multiple sub elements or
using instanceOf check).

.. code-block:: java
  :caption: Zoom on **ClientAbstract** GET way (receiving InputStream from server)

    /**
     * @param context 1 for sending InputStream, -1 for receiving InputStream, or anything else
     * @return the headers map
     */
    protected abstract Map<String, String> getHeadersFor(I businessIn, int context);

    /**
     * @return the BusinessOut from the response content and/or headers
     */
    protected abstract O getApiBusinessOutFromResponse(final Response response);



Client definition of Service
=================================

Note that **ApiServiceInterface** is the API of the server, with specific attention on InputStream,
using a different Java Interface than the server's one. This is due to the need to access to low level
injected values such as ``HttpServerRequest`` and ``Closer``.

Note: these declarations are not useful since the client service will never be used for those end points.

.. code-block:: java
  :caption: Example test code for **ApiServiceInterface** (client side)

  @Path(ApiConstants.API_COLLECTIONS)
  @POST
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  @Produces(MediaType.APPLICATION_JSON)
  Uni<Response> createObject(InputStream content,
      @DefaultValue("name") @RestHeader(ApiConstants.X_NAME) String name,
      @DefaultValue("0") @RestHeader(ApiConstants.X_LEN) long len);

  @Path(ApiConstants.API_COLLECTIONS + "/{business}")
  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  Uni<InputStream> readObject(@RestPath String business);



Server definition of Service
=================================

Be careful that API using InputStream (push or pull) are defined with the annotation ``@Blocking`` on server side.

.. code-block:: java
  :caption: Example test code for **ApiService** (server side)

  @Path(API_COLLECTIONS)
  @POST
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  public Uni<Response> createObject(HttpServerRequest request, @Context final Closer closer,
      final InputStream inputStream,
      @DefaultValue("name") @RestHeader(X_NAME) String name,
      @DefaultValue("0") @RestHeader(X_LEN) long len) {
    ApiBusinessIn businessIn = new ApiBusinessIn();
    businessIn.name = name;
    businessIn.len = len;
    return createObject(request, closer, businessIn, businessIn.len, null, keepCompressed, inputStream);
  }

  @Path(API_COLLECTIONS + "/{business}")
  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Blocking
  public Uni<Response> readObject(@RestPath final String business,
      final HttpServerRequest request, @Context final Closer closer) {
    ApiBusinessIn businessIn = new ApiBusinessIn();
    businessIn.name = business;
    String xlen = request.getHeader(X_LEN);
    long len = LEN;
    if (ParametersChecker.isNotEmpty(xlen)) {
      len = Long.parse(xlen);
    }
    businessIn.len = len;
    return readObject(request, closer, businessIn, futureAlreadyCompressed);
  }

- ``keepInputStreamCompressed`` specifies for each end point if the InputStream shall be kept
  compressed if already compressed, or uncompressed if compressed.

**The Client Factory should be used as ``@ApplicationScoped`` in order to ensure it is always the unique one.**


Client implementation
======================

.. code-block:: java
  :caption: Example test code for **ApiClient**

  public ApiBusinessOut postInputStream(final String name, final InputStream content,
      final long len, final boolean shallCompress, final boolean alreadyCompressed) throws CcsWithStatusException {
    ApiBusinessIn businessIn = new ApiBusinessIn();
    businessIn.name = name;
    businessIn.len = len;
    final var inputStream = prepareInputStreamToSend(content, shallCompress, alreadyCompressed, businessIn);
    final var uni = getService().createObject(name, len, inputStream);
    return getResultFromPostInputStreamUni(uni, inputStream);
  }

  public InputStreamBusinessOut<ApiBusinessOut> getInputStream(final String name, final long len,
      final boolean acceptCompressed, final boolean shallDecompress)
      throws CcsWithStatusException {
    ApiBusinessIn businessIn = new ApiBusinessIn();
    businessIn.name = name;
    businessIn.len = len;
    prepareInputStreamToReceive(acceptCompressed, businessIn);
    final var uni = getService().readObject(name);
    return getInputStreamBusinessOutFromUni(acceptCompressed, shallDecompress, uni);
  }

- ``shallCompress`` and ``acceptCompressed`` specify if the InputStream must be compressed (either in POST or GET).

- ``alreadyCompressed`` specifies if the InputStream is already compressed or not in POST.

- ``shallDecompress`` specifies if the InputStream shall be decompressed if received compressed.

Client implementation using Quarkus Service
==============================================

It is possible to use native Quarkus client. (service is the injected ApiService with correct
URL from ``quarkus.rest-client."org.acme.rest.client.ExtensionsService".url=yourUrl``).

.. code-block:: java
  :caption: Example test code for **ApiClient** using service

  public class ApiClient extends ClientAbstract<ApiBusinessIn, ApiBusinessOut, ApiServiceInterface> {
    public boolean checkName(final String name) {
      final Uni<Response> uni = getService().checkName(name);
      ApiBusinessIn businessIn = new ApiBusinessIn();
      businessIn.name = name;
      try (final Response response = exceptionMapper.handleUniResponse(uni)) {
        return name.equals(response.getHeaderString(X_NAME));
      } catch (final CcsClientGenericException | CcsServerGenericException | CcsWithStatusException e) {
        return false;
      }
    }
    ...
  }

Some helpers are created to make it easier to handle the return status.

.. code-block:: java
  :caption: Example test code for **ExceptionMapper** helper

    // Response format
    final Uni<Response> uni = getService().checkName(name);
    try (final Response response = exceptionMapper.handleUniResponse(uni)) {
      // OK
    } catch (final CcsClientGenericException | CcsServerGenericException | CcsWithStatusException e) {
      // Handle exception
    }

    // DTO format
    final var uni = getService().getObjectMetadata(name);
    return (ApiBusinessOut) exceptionMapper.handleUniObject(this, uni);

Note that if a Factory is going to be used for several targets, the factory is then not correctly initialized with the
right URI. Therefore the following example shall be followed:

.. code-block:: java
  :caption: Example code for **ApiClientFactory and ApiClient** with multiple targets

    // Still get the Factory using @Inject
    @Inject
    ApiClientFactory factory;

    // Then in method where the client is needed for a particular URI
    try (final ApiClient apiClient = factory.newClient(uri)) {
      // This method is synchronized on Factory to prevent wrong setup
      // (getUri() will return the right URI at construction but not guaranteed later on)
    }


Server implementation
=================================

- ``io.clonecloudstore.common.quarkus.server`` contains the StreamHandlerAbstract, the StreamServiceAbstract and some filters implementations for the server.

With those abstracts, the code needed is shortest and allow to be extended to any API and usages.

The abstract **StreamServiceAbstract** defines abstract methods, as **StreamHandlerAbstract**, that must be
specified within the final client implementation, in order to include business implementation.

.. code-block:: java
  :caption: Zoom on abstract methods in **StreamHandlerAbstract** helper for InputStream received by the server

    /**
     * @return True if the digest is to be computed on the fly
     */
    protected abstract boolean checkDigestToCumpute(I businessIn);

    /**
     * Check if the request for POST is valid, and if so, adapt the given MultipleActionsInputStream that will
     * be used to consume the original InputStream.
     * The implementation shall use the business logic to check the validity for this InputStream reception
     * (from client to server) and, if valid, use the MultipleActionsInputStream, either as is or as a standard InputStream.
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

.. graphviz::
  :caption: Illustration of network steps in receiving InputStream within server

  digraph dependencies {
    "servicePost" -> "nativeStreamHandler" [label="Creation"];
    "nativeStreamHandler" -> "headersCheck" [label="Check compression"];
    "headersCheck" -> "checkPushAble" [label="Check if push is allowed"];
    "checkPushAble" -> "asyncInputStreamConsumer" [label="InputStream consumption through new thread and create a semaphore"];
    "checkPushAble" -> "asyncInputStreamProducer" [label="InputStream producing from Network input"];
    "asyncInputStreamConsumer" -> "externalUsage" [label="InputStream consumption through new thread"];
    "externalUsage" -> "semaphoreRelease" [label="Release semaphore when consumption ending"];
    "asyncInputStreamProducer" -> "inputStreamReception" [label="Feeding InputStream from Network"];
    "inputStreamReception" -> "endProducing" [label="Finalize producing InputStream"];
    {"semaphoreRelease" "endProducing"} -> "getAnswerPushInputStream" [label="Once production and consumption ending"];
    "getAnswerPushInputStream" -> "finalizeConsumer" [label="Ending consumer once everything is done"];
    "finalizeConsumer" ->  "getHeaderPushInputStream" [label="Get final headers for response"];
    "getHeaderPushInputStream" -> "finalAnswer" [label="Final answer"];
  }

.. code-block:: java
  :caption: Zoom on abstract methods in **NativeStreamHandler** helper for InputStream sent by the server

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


.. graphviz::
  :caption: Illustration of network steps in sending InputStream within server

  digraph dependencies {
    "serviceGet" -> "nativeStreamHandler" [label="Creation"];
    "nativeStreamHandler" -> "headersCheck" [label="Check if push is allowed"];
    "headersCheck" -> "checkPullAble" [label="Check if pull is allowed"];
    "checkPullAble" -> "getPullInputStream" [label="InputStream and Metadata creation using async operation for InputStream"];
    "getPullInputStream" -> "asyncInputStreamProducer" [label="InputStream producing from external resource"];
    "getPullInputStream" -> "getHeaderPullInputStream" [label="Get final headers for response from Metadata"];
    "getHeaderPullInputStream" -> "headerSend" [label="Send headers as answer"];
    "asyncInputStreamProducer" -> "inputStreamEmission" [label="Async emission as response from InputStream"];
    "inputStreamEmission" -> "endProducing" [label="End of InputStream sending"];
    {"headerSend" "endProducing"} -> "finalAnswer" [label="Final answer"];
  }


.. code-block:: java
  :caption: Zoom on abstract methods in **NativeStreamHandler** helper for error message (in or out)

    /**
     * Return headers for error message.
     * (example: get headers in case of error as Object name, Bucket name...)
     */
    protected abstract Map<String, String> getHeaderError(I businessIn, int status);

Note that if several API are intended for this server to send or receive InputStream (various usages), one shall specialized the answer
of those abstract methods through more general BusinessIn and BusinessOut types (for instance, using multiple sub elements or
using instanceOf check).


.. code-block:: java
  :caption: Example test code for **ApiService** (Class definition and REST service definition)

  @ApplicationScoped
  @Path(API_ROOT)
  public class ApiService
      extends StreamServiceAbstract<ApiBusinessIn, ApiBusinessOut, NativeStreamHandler> {


The interaction with a Driver is done through the extension of **StreamHandlerAbstract**.


.. code-block:: java
  :caption: Example test code for **NativeStreamHandler**

  @RequestScoped
  public class NativeStreamHandler
      extends StreamHandlerAbstract<ApiBusinessIn, ApiBusinessOut> {
    public NativeStreamHandler() {
    }
    // Implement abstract methods
  }


TrafficShaping
***************

Limiting traffic on network (or any other resource) could be difficult natively.
This aims to propose a simple solution.

Since Quarkus implements natively trafficShaping, the project will use this default one.


JsonUtil
*********

Since ObjectMapper from Jackson library is often needed for manual integration,
this helper returns an ObjectMapper:

- If Quarkus has initialized it, the one from Quarkus
- If not, a default one, almost equivalent

