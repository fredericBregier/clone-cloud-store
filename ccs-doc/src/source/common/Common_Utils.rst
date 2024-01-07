Common Standard
################

Common Standard is meant for "non Quarkus" usage (does not implied Quarkus libraries), while Common Utils is meant for
"Quarkus" context, relying on Common Standard too.

GuidLike and relative Guid
******************************

Prefer to use GUID instead of UUID since UUID are not able to be allocated without collision between
several independent instances.

One proposed implementation is the **GuidLike**. It uses by default the MAC Address and the PID of the
Java process, plus a tenant, the time and an internal counter.

.. code-block:: java
  :caption: Example code for **GuidLike**

  // Create Guid simply
  String guid = new GuidLike().getId();
  String guid = GuidLike.getGuid(); // Equivalent

Those Guid could be used in particular when any unique Id is needed, for instance in database model.

For simple Uuid (not Guid), there is also the LongUuid implementation (uniqueness cannot be ensure across several JVM,
neither several regions).

.. code-block:: java
  :caption: Example code for **LongUuid**

  LongUuid uuid = new LongUuid();
  // If hexa form is needed
  String hexa = uuid.getId();
  // If long form is needed
  long id = uuid.getLong();
  // or quicker
  long id2 = LongUuid.getLongUuid();


BaseXx
********

This is simple encapsulation of other libraries to simplify usage:


.. code-block:: java
  :caption: Example code for **BaseXx**

    final String encoded64 = BaseXx.getBase64("FBTest64P".getBytes());
    final byte[] bytes64 = BaseXx.getFromBase64(encoded64);
    final String encoded32 = BaseXx.getBase32("FBTest32".getBytes());
    final byte[] bytes32 = BaseXx.getFromBase32(encoded32);
    final String encoded16 = BaseXx.getBase16("FBTest16".getBytes());
    final byte[] bytes16 = BaseXx.getFromBase16(encoded16);


Various X InputStream
**********************

- **ChunkInputStream** : From one InputStream, cut into sub-InputStream with a fix length
  (useful for multipart support for Object Storage for InputStream greater than 5 GB)
- **MultipleActionsInputStream**: Can compute one Digest from an InputStream, while reading it,
  and compress or decompress using ZSTD algorithm
- **FakeInputStream**: Only usable in test, create a fake InputStream for a given
  length with no memory impact (except one buffer) (allows to create 2 TB InputStream
  for instance). This one is placed in **ccs-test-stream** module since test only related.
- **TeeInputStream**: Used to consume twice (or more) one InputStream. Note that the overall speed will be the
  slowest consumer speed. If one consumer is closing its own InputStream, it will not affect the others.

.. code-block:: java
  :caption: Example code for **TeeInputStream**

  int nbTee = x;
  try (FakeInputStream fakeInputStream = new FakeInputStream(INPUTSTREAM_SIZE, (byte) 'a');
       TeeInputStream teeInputStream = new TeeInputStream(fakeInputStream, nbTee)) {
    InputStream is;
    final Future<Integer>[] total = new Future[nbTee];
    final ExecutorService executor = Executors.newFixedThreadPool(nbTee);
    for (int i = 0; i < nbTee; i++) {
      is = teeInputStream.getInputStream(i);
      final ThreadReader threadReader = new ThreadReader(i, is, size);
      total[i] = executor.submit(threadReader);
    }
    executor.shutdown();
    while (!executor.awaitTermination(10000, TimeUnit.MILLISECONDS)) {
      // Empty
    }
    for (int i = 0; i < nbTee; i++) {
      assertEquals(INPUTSTREAM_SIZE, (int) total[i].get());
    }
    // If one wants to know if any of the underlying threads raised an exception on their own InputStream
    teeInputStream.throwLastException();
    // teeInputStream.close() implicit since in Try resource
  } catch (final InterruptedException | ExecutionException | IOException e) {
    LOGGER.error(e);
    fail("Should not raised an exception: " + e.getMessage());
  }


ZstdCompressInputStream and ZstdDecompressInputStream
------------------------------------------------------

ZSTD (Zstandard) is a modern and efficient compression (both in time, memory and compression).

Those InputStreams allows to compress or decompress on the flY.

General usages should be that those compression / decompression

.. code-block:: java
  :caption: Example code for **ZstdCompressInputStream and ZstdDecompressInputStream**

    final ZstdCompressInputStream zstdCompressInputStream = new ZstdCompressInputStream(inputStream);
    // Here TrafficShaping is applied once compression is done, and before decompression, as if there were a
    // trafficShaping between sending InputStream and receiving InputStream (wire handling)
    final var trafficShapingInputStream = new TrafficShapingInputStream(zstdCompressInputStream, trafficShaping);
    // Supposedly here: wire transfer
    final ZstdDecompressInputStream zstdDecompressInputStream =
        new ZstdDecompressInputStream(trafficShapingInputStream);
    int read;
    while ((read = zstdDecompressInputStream.read(bytes, 0, bytes.length)) >= 0) {
      // Do something with the decompressed InpuStream
    }
    zstdCompressInputStream.close();
    zstdDecompressInputStream.close();


ParametersChecker
******************

Can be used for String (testing also emptiness) and for general Object.
For null String only, use the special method.

It allows also some general sanity check to prevent wrong data in entry (such as
CDATA or ENTITY in xml, SCRIPTS in Javascript, ``;`` in sql parameters...).
2 special methods ``checkSanityBucketName(name)`` and ``checkSanityObjectName(name)`` are intended to ensure
correctness of such names when using Object Storage.

This could be later on extended to use external library specialized in sanity check (such as the Owasp library).

I also includes a special function to fix Instant to milliseconds, instead of 1000 nanoseconds, since most of
the database cannot handle more than millisecond.

Various Random
****************

It could be useful (and in particular for Guid) to get random values in an efficient
way or in a secure way (a bit less efficient but still efficient).

- **RandomUtil** helps to get efficient Random values
- **SystemRandomSecure** helps to get efficient and secure Random values.


Singleton
**********

Utility class to get standard Singleton (empty and unmodifiable object), such as:

- Empty byte array
- Empty List
- Empty Set
- Empty Map
- Empty InputStream
- Empty OutputStream (moved to **ccs-test-stream** module since test only related)


SysErrLogger
*************

In some rare case, we cannot have a Logger due to the fact the initialization is not done.

In some other case, for quality code reasons, while we do not need to log anything in a catched
exception, it is useful to set a log (but we do not want an output).

This is where the SysErrLogger comes.


.. code-block:: java
  :caption: Example code for **SysErrLogger**

  try {
    something raising an exception
  } catch (final Exception ignore) {
    // This exception shall be ignored
    SysErrLogger.FAKE_LOGGER.ignoreLog(ignore);
  }
  // Output to SysErr without Logger
  SysErrLogger.FAKE_LOGGER.syserr(NOT_EMPTY, new Exception("Fake exception"));
  // Output to SysOut without Logger
  SysErrLogger.FAKE_LOGGER.sysout(NOT_EMPTY);


System Properties and Quarkus Configuration
********************************************

We need sometimes to get configuration (Quarkus) or System Properties statically
and not through injection.

.. code-block:: java
  :caption: Example code for **SystemPropertyUtil**

  SystemPropertyUtil.get(KEY, defaultValue);
  SystemPropertyUtil.getAndSet(KEY, defaultValue);
  SystemPropertyUtil.set(KEY, defaultValue);
  // Quarkus Configuration
  SystemPropertyUtil.getBooleanConfig(KEY)
  SystemPropertyUtil.getStringConfig(KEY);
  SystemPropertyUtil.getLongConfig(KEY);
  SystemPropertyUtil.getBooleanConfig(KEY);
