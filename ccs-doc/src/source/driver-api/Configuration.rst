Specific Driver configurations
###################################

.. warning::
  Note for S3 that ``maxPartSizeForUnknownLength`` or ``driverMaxChunkSize`` should be defined according to memory available
  and concurrent access, as each transfer (upload or download) could lead to one buffer of this size for each.

.. list-table:: Driver for S3 Service Configuration
   :header-rows: 1
   :widths: 1 2

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
   :widths: 1 2

   * - Property/Yaml property
     - Possible Values
   * - ``quarkus.azure.storage.blob.connection-string``
     - Connection String to Azure Blob Storage (see https://docs.quarkiverse.io/quarkus-azure-services/dev/index.html)
   * - ``ccs.driver.azure.maxConcurrency``
     - ``2``, Maximum concurrency in upload/download with Azure Blob Storage
   * - ``ccs.driver.azure.maxPartSize``
     - 256 MB, MultiPart size (minimum 5 MB, maximum 4 GB, default 256 MB)
   * - ``ccs.driver.azure.maxPartSizeForUnknownLength``
     - 512 MB as in ``ccs.driverMaxChunkSize``, MultiPart size (minimum 5 MB, maximum ~2 GB): will be used to buffer InputStream if length is unknown (no memory impact)


.. list-table:: Driver for Google Cloud Storage Service Configuration
   :header-rows: 1
   :widths: 5 8

   * - Property/Yaml property
     - Possible Values
   * - ``quarkus.google.cloud.project-id``
     - Project Id in Google Cloud (and related Authentication see https://docs.quarkiverse.io/quarkus-google-cloud-services/main/index.html)
   * - ``ccs.driver.google.disableGzip``
     - ``true``, Default is to use Gzip content, but may be disabled (default: true so disabled)
   * - ``ccs.driver.google.maxPartSize``
     - 256 MB, MultiPart size (minimum 5 MB, maximum 4 GB, default 256 MB) (**Property ignored**)
   * - ``ccs.driver.google.maxBufSize``
     - 128 MB; MultiPart size (minimum 5 MB, maximum ~2 GB): will be used to buffer InputStream if length is unknown (no memory impact)

