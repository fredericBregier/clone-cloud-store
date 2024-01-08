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

- ``io.clonecloudstore.common.quarkus.server`` contains the NativeStreamHandlerAbstract, the StreamServiceAbstract and some filters implementations for the server.

With those abstracts, the code needed is shortest and allow to be extended to any API and usages.

The abstract **StreamServiceAbstract** defines abstract methods, as **NativeStreamHandlerAbstract**, that must be
specified within the final client implementation, in order to include business implementation.

.. code-block:: java
  :caption: Zoom on abstract methods in **NativeStreamHandlerAbstract** helper for InputStream received by the server

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


The interaction with a Driver is done through the extension of **NativeStreamHandlerAbstract**.


.. code-block:: java
  :caption: Example test code for **NativeStreamHandler**

  @RequestScoped
  public class NativeStreamHandler
      extends NativeStreamHandlerAbstract<ApiBusinessIn, ApiBusinessOut> {
    public NativeStreamHandler() {
    }
    // Implement abstract methods
  }



PriorityQueue
**************

It might be necessary to handle a PriorityQueue locally with the ability to
manage re-prioritization (as done in the Linux scheduler).

Several functions (lambda) shall be passed at construction time:

.. code-block:: java
  :caption: Example code for **PriorityQueue** creation

  private static final Comparator<ElementTest> comparator = Comparator.comparingLong(o -> o.rank);
  private static final Comparator<ElementTest> findEquals = Comparator.comparing(o -> o.uniqueId);
  private static final UnaryOperator<ElementTest> reprioritize = e -> {
    e.rank /= 2;
    return e;
  };
  private static final int MAX_RUNNABLE = 10;
  ManagedRunnable<ElementTest> managedRunnable =
      new ManagedRunnable<>(MAX_RUNNABLE, comparator, findEquals, reprioritize);


- ``MAX_RUNNABLE`` is the number of priority elements that will be managed in a
  round-robin way equally.
- ``comparator`` is the function to compare 2 items to find which one is priority
  (lowest will be placed at first position).
- ``findEquals`` is the function to find an item equals to the one passed as argument
  (equality can be based on something different than objects are really the same).
- ``reprioritize`` is the function that traverse all items (not already in short list live) to reorder them
  accordingly to the new priority.

The main point is that the queue is split in 2, in order to not have too much
items running (or active) at the same time. This is fixed by the maxRunnable parameter.
Once items are ordered according to priority, this parameter allows to consume those
number of items in a round robin way.


.. code-block:: java
  :caption: Example code for **PriorityQueue** usage

  // One can add items 1 by 1
  managedRunnable.add(new ElementTest(10));
  // This one will have a lower priority
  managedRunnable.add(new ElementTest(20));
  // One can add items from a collection
  List<ElementTest> list = new ArrayList<>();
  // This one will be latest one
  list.add(new ElementTest(50));
  // This one will be the first
  list.add(new ElementTest(5));
  managedRunnable.addAll(list);
  // Now we can start to consume
  while (!managedRunnable.isEmpty()) {
    e = managedRunnable.poll();
    if (e != null) { // Queue might be empty when poll is called
      // Do something with e
      ...
      // If e needs to regain Queue as active runner
      if (myTaskNeedsToContinue(e)) {
        // Item will return to active items (round-robin)
        managedRunnable.addContinue(e);
      } else if (myTaskNeedsToBeReprioritize(e)) {
        // Here the item will be add at the end (out of round-robin)
        managedRunnable.add(e);
      }
    }
  }

One usage could be to select among a lot of actions to be done the top 10
to apply, and then poll out next ones when able to do so.

To prevent that an old entry is never planned, the ``reprioritize`` is called
each time one element will be taken out of the round robin sub-queue, such that
it has a chance to become the most priority one.


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


StateMachine
**************

StateMachine package allows to handle simple State Machine with various steps.


.. code-block:: java
  :caption: Example code for **StateMachine**

  // States definition
  public enum ExampleEnumState {
    NONE, START, RUNNING, PAUSE, END, ERROR;

    static final List<StateTransition<ExampleEnumState>> configuration;

    static {
      ExampleStateTransition[] stateTransitions = ExampleStateTransition.values();
      configuration = new ArrayList<>(stateTransitions.length);
      for (final ExampleStateTransition stateTransition : stateTransitions) {
        configuration.add(stateTransitions.elt);
      }
    }

    public static StateMachine<ExampleEnumState> newStateMachine() {
      return new StateMachine<>(NONE, configuration);
    }
  }
  // StateTransition definition
  public enum ExampleStateTransition {
    tNONE(NONE, START, EnumSet.of(START)),
    tSTART(START, RUNNING, EnumSet.of(RUNNING, PAUSE, ERROR)),
    tRUNNING(RUNNING, END, EnumSet.of(PAUSE, END, ERROR)),
    tPAUSE(PAUSE, RUNNING), tEND(END), tERROR(ERROR);

    public final StateTransition<ExampleEnumState> elt;

    ExampleStateTransition(final ExampleEnumState state) {
      elt = new StateTransition<>(state);
    }

    ExampleStateTransition(final ExampleEnumState state,
                           final ExampleEnumState stateNext) {
      elt = new StateTransition<>(state, stateNext);
    }

    ExampleStateTransition(final ExampleEnumState state,
                           final ExampleEnumState stateNext,
                           final EnumSet<ExampleEnumState> set) {
      elt = new StateTransition<>(state, stateNext, set);
    }
  }
  // Example of usages
  final StateMachine<ExampleEnumState> stateMachine =
        ExampleEnumState.newStateMachine();
  stateMachine.getState(); // NONE
  stateMachine.isReachable(END); // False
  stateMachine.setDryState(END); // NONE
  stateMachine.isReachable(START); // True
  stateMachine.setState(START); // START
  stateMachine.isTerminal(); // False

