Driver API
###########

The Driver API is the core of integration of multiple Object Storage solutions.
Each integration must implement this interface in order to be able to add this as
a plugin within Cloud Cloud Store.

Global logic of API
=======================

.. graphviz::
  :caption: Relation between Cloud Cloud Store, Driver and Object Storage

  digraph dependencies {
    subgraph {
      "ObjectStorage" [color = gray]
    }
    subgraph {
      "Driver" [color = green]
    }
    subgraph {
      "CCS-Standard" [color = brown];
    }
    subgraph {
      "CCS-Quarkus" [color = brown];
    }

    "CCS-Standard" -> "Driver";
    "CCS-Quarkus" -> "Driver";
    "Driver" -> "ObjectStorage";
  }

3 implementations
=====================

There are 3 implementations:

* S3 like support (whatever Amazon, Minio or any S3 compatible implementations)
* Azure Blob Storage support
* Google Cloud Storage support

All of these implementations respect the API of the Driver.

Driver API details
======================

The DriverApiFactory might be created by explicitly create the right implementation (for instance
``Arc.container().select(DriverApiFactory.class).get();``).

The DriverApi is then created through this Factory with ``factory.getInstance()``.

However, each DriverApiFactory must register once configured within DriverApiRegister, such that the factory is
get using ``DriverApiRegister.getDriverApiFactory()``.

Note that all methods accept both String for Bucket / Object and StorageBucket / StorageObject, except
``bucketCreate`` and ``objectPrepareCreateInBucket`` since those methods need a full StorageBucket or StorageObject.

For instance: ``bucketDelete(String bucket)`` can be written as ``bucketDelete(StorageBucket bucket)``, or
``objectGetInputStreamInBucket(String bucket, String object)`` as ``objectGetInputStreamInBucket(StorageObject object)``

Bucket operations
++++++++++++++++++

.. code-block:: java
  :caption: Java API for **Buckets**

  /**
   * Count Buckets
   */
  long bucketsCount() throws DriverException;

  /**
   * List Buckets
   */
  Stream<StorageBucket> bucketsList() throws DriverException;

.. code-block:: java
  :caption: Java API for **Bucket**

  /**
   * Create one Bucket and returns it
   *
   * @param bucket contains various information that could be implemented within Object Storage, but, except the name
   *               of the bucket, nothing is mandatory
   * @return the StorageBucket as instantiated within the Object Storage (real values)
   */
  StorageBucket bucketCreate(StorageBucket bucket)
      throws DriverNotAcceptableException, DriverAlreadyExistException, DriverException;

  /**
   * Delete one Bucket if it exists and is empty
   */
  void bucketDelete(String bucket) throws DriverNotAcceptableException, DriverNotFoundException, DriverException;

  /**
   * Check existence of Bucket
   */
  boolean bucketExists(String bucket) throws DriverException;

Object operations
++++++++++++++++++

.. code-block:: java
  :caption: Java API for **Objects**

  /**
   * Count Objects in specified Bucket
   */
  long objectsCountInBucket(final String bucket) throws DriverNotFoundException, DriverException;

  /**
   * Count Objects in specified Bucket with filters (all optionals)
   */
  long objectsCountInBucket(String bucket, String prefix, Instant from, Instant to)
      throws DriverNotFoundException, DriverException;

  /**
   * List Objects in specified Bucket.
   */
  Stream<StorageObject> objectsListInBucket(String bucket)
      throws DriverNotFoundException, DriverException;

  /**
   * List Objects in specified Bucket with filters (all optionals)
   */
  Stream<StorageObject> objectsListInBucket(String bucket, String prefix, Instant from, Instant to)
      throws DriverNotFoundException, DriverException;

.. code-block:: java
  :caption: Java API for **Object**

  /**
   * Check if Directory or Object exists in specified Bucket (based on prefix)
   */
  StorageType directoryOrObjectExistsInBucket(final String bucket, final String directoryOrObject)
      throws DriverException;

  /**
   * First step in creation of an object within a Bucket. The InputStream is ready to be read in
   * a concurrent independent thread to be provided by the driver. Sha256 might be null or empty. Len might be 0,
   * meaning unknown.
   *
   * @param object contains various information that could be implemented within Object Storage, but, except the name
   *               of the bucket and the key of the object, nothing is mandatory
   */
  void objectPrepareCreateInBucket(StorageObject object, InputStream inputStream)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException;

  /**
   * Second step in creation of an object within a Bucket. Sha256 might be null or empty. Reallen must not be 0.
   * This method waits for the prepare method to end and returns the final result.
   *
   * @return the StorageObject as instantiated within the Object Storage (real values)
   */
  StorageObject objectFinalizeCreateInBucket(String bucket, String object, long realLen, String sha256)
      throws DriverNotFoundException, DriverAlreadyExistException, DriverException;

  /**
   * Get the content of the specified Object within specified Bucket
   */
  InputStream objectGetInputStreamInBucket(String bucket, String object) throws DriverNotFoundException,
      DriverException;

  /**
   * Get the Object metadata from this Bucket (those available from Object Storage)
   */
  StorageObject objectGetMetadataInBucket(String bucket, String object)
      throws DriverNotFoundException, DriverException;

  /**
   * Delete the Object from this Bucket
   */
  void objectDeleteInBucket(String bucket, String object)
      throws DriverNotAcceptableException, DriverNotFoundException, DriverException;
