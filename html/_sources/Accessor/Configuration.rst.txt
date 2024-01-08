Configuration
*************

** TODO **

Various Accessor services
===========================

Accessor-Replicator
++++++++++++++++++++

Used by remote request from Replicator. It listens to a Topic ``replicator-action-in`` from (local) Remote Replicator and is
able, through the Local Replicator to access to remote Object if needed for local creation (clone)
or to Accessor Internal Service.

This service has no API and only uses a Topic as incoming requests.

Accessor-Simple-Gateway
+++++++++++++++++++++++

Simple Cloud Storage Gateway without any database and replication nor reconciliation support.

It is means for an easy move from existing storage to Cloud Clone Store, to later on apply the real Accessor service.

Accessor-Server
+++++++++++++++

Used by application (clients) to interact with Cloud Storage, enabling replication and remote access and reconciliation,
using the public API.

It is also used internally by all services when they need to access or interact with buckets and objects, through the
internal API, which must be not available to other services than Cloud Clone Store itself.


application.yaml configuration
===============================

.. list-table:: Accessor Cloud Clone Store Client Configuration
   :header-rows: 1

   * - Property/Yaml property
     - Possible Values
     - Default Value
   * - ``quarkus.rest-client."io.clonecloudstore.accessor.client.api.AccessorBucketApi".url``
     - Http(s) url of the service
     -
   * - ``quarkus.rest-client."io.clonecloudstore.accessor.client.api.AccessorObjectApi".url``
     - Http(s) url of the service
     -


.. list-table:: Accessor Cloud Clone Store Internal Client Configuration
   :header-rows: 1

   * - Property/Yaml property
     - Possible Values
     - Default Value
   * - ``quarkus.rest-client."io.clonecloudstore.accessor.client.internal.api.AccessorBucketInternalApi".url``
     - Http(s) url of the service
     -
   * - ``quarkus.rest-client."io.clonecloudstore.accessor.client.internal.api.AccessorObjectInternalApi".url``
     - Http(s) url of the service
     -


.. list-table:: Accessor Replicator Cloud Clone Store Service Configuration
   :header-rows: 1

   * - Property/Yaml property or Environment variable
     - Possible Values
     - Default Value
   * - ``ccs.accessor.site``
     - Name of the site
     - ``unconfigured``
   * - ``ccs.accessor.internal.compression``
     - ``true`` or ``false``, True to allow compression between services
     - ``false``
   * - Redefining ``mp.messaging.incoming.replicator-action-in`` or env ``CCS_REQUEST_ACTION``
     - Name of the incoming topic for Action Requests
     - ``request-action``
   * - ``quarkus.mongodb.database``
     - Name of the associated database (if MongoDB used, with ccs.db.type = mongo)
     -
   * - ``quarkus.rest-client."io.clonecloudstore.replicator.client.api.LocalReplicatorApi".url``
     - Http(s) url of the service
     -


.. list-table:: Accessor Cloud Clone Store Service Configuration
   :header-rows: 1

   * - Property/Yaml property or Environment variable
     - Possible Values
     - Default Value
   * - ``ccs.accessor.site``
     - Name of the site
     - ``unconfigured``
   * - ``ccs.accessor.remote.read``
     - ``true`` or ``false``, True to allow remote access when object not locally found
     - ``false``
   * - ``ccs.accessor.remote.fixOnAbsent``
     - ``true`` or ``false``, True to allow to fix using remote accessed object
     - ``false``
   * - ``ccs.accessor.internal.compression``
     - ``true`` or ``false``, True to allow compression between services
     - ``false``
   * - Redefining ``mp.messaging.outgoing.replicator-action-out`` or env ``CCS_REQUEST_ACTION``
     - Name of the outgoing topic for Action Requests
     - ``request-action``
   * - Redefining ``mp.messaging.outgoing.replicator-request-out`` or env ``CCS_REQUEST_REPLICATION``
     - Name of the outgoing topic for Replication Requests
     - ``request-replication``
   * - ``quarkus.mongodb.database``
     - Name of the associated database (if MongoDB used, with ccs.db.type = mongo)
     -
   * - ``quarkus.rest-client."io.clonecloudstore.replicator.client.api.LocalReplicatorApi".url``
     - Http(s) url of the service
     -

For both *Accessor Replicator Cloud Clone Store Service* and *Accessor Cloud Clone Store Service*,
an extra configuration is needed according to the Storage Driver used:

- Note that ``maxPartSizeForUnknownLength`` should be defined according to memory available and concurrent access, as
  each transfer (upload or download) could lead to one buffer of this size for each.

.. list-table:: Driver for S3 Service Configuration
   :header-rows: 1

   * - Property/Yaml property
     - Possible Values
   * - ``ccs.driver.s3.host``
     - S3 Host (do not use ``quarkus.s3.endpoint-override``)
   * - ``ccs.driver.s3.keyId``
     - S3 KeyId (do not use ``quarkus.s3.aws.credentials.static-provider.access-key-id`` nor ``aws.accessKeyId``)
   * - ``ccs.driver.s3.key``
     - S3 Key (do not use ``quarkus.s3.aws.credentials.secret-access-key`` nor ``aws.secretAccessKey``)
   * - ``ccs.driver.s3.region``
     - S3 Region (do not use ``quarkus.s3.aws.region``)
   * - ``ccs.driver.s3.maxPartSize``
     - MultiPart size (minimum 5 MB, maximum 5 GB, default 256 MB)
   * - ``ccs.driver.s3.maxPartSizeForUnknownLength``
     - 512 MB as in ``ccs.driverMaxChunkSize``, MultiPart size (minimum 5 MB, maximum ~2 GB): will be used to buffer InputStream if length is unknown, so take  care of the Memory consumption associated (512 MB, default, will limit the total InputStream length to 5 TB since 10K parts)


.. list-table:: Driver for Azure Blob Storage Service Configuration
   :header-rows: 1

   * - Property/Yaml property
     - Possible Values
   * - ``quarkus.azure.storage.blob.connection-string``
     - Connection String to Azure Blob Storage (see https://docs.quarkiverse.io/quarkus-azure-services/dev/index.html)
   * - ``ccs.driver.azure.maxConcurrency``
     - ``2``, Maximum concurrency in upload/download with Azure Blob Storage
   * - ``ccs.driver.azure.maxPartSize``
     - 256 MB, MultiPart size (minimum 5 MB, maximum 4 GB, default 256 MB)
   * - ``ccs.driver.azure.maxPartSizeForUnknownLength``
     - 512 MB as in ``ccs.driverMaxChunkSize``, MultiPart size (minimum 5 MB, maximum ~2 GB): will be used to buffer InputStream if length is unknown, so take  care of the Memory consumption associated (512 MB, default, will limit the total InputStream length to 25 TB since 50K parts)


.. list-table:: Driver for Google Cloud Storage Service Configuration
   :header-rows: 1

   * - Property/Yaml property
     - Possible Values
   * - ``quarkus.google.cloud.project-id``
     - Project Id in Google Cloud (and related Authentication see https://docs.quarkiverse.io/quarkus-google-cloud-services/main/index.html)
   * - ``ccs.driver.google.disableGzip``
     - ``true``, Default is to use Gzip content, but may be disabled (default: true so disabled)
   * - ``ccs.driver.google.maxPartSize``
     - 256 MB, MultiPart size (minimum 5 MB, maximum 4 GB, default 256 MB) (**Property ignored**)
   * - ``ccs.driver.google.maxBufSize``
     - 128 MB; MultiPart size (minimum 5 MB, maximum ~2 GB): will be used to buffer InputStream if length is unknown, so take  care of the Memory consumption associated (128 MB, default)


.. list-table:: Accessor Simple Gateway Cloud Clone Store Service Configuration
   :header-rows: 1

   * - Property/Yaml property
     - Possible Values
     - Default Value
   * - ``ccs.accessor.site``
     - Name of the site
     - ``unconfigured``

