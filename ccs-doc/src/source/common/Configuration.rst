Common Configuration
*********************

Several parts are concerned by the configuration.

Here are the global parameters, whatever the service.

``application.yaml`` configuration
==================================

The following parameters are for optimization.

.. list-table:: Common Quarkus Configuration
   :header-rows: 1

   * - Property/Yaml property
     - Default Value
     - Comment
   * - ``quarkus.http.so-reuse-port``
     - ``true``
     - Optimization on Linux/MacOs
   * - ``quarkus.http.tcp-cork``
     - ``true``
     - Optimization on Linux
   * - ``quarkus.http.tcp-quick-ack``
     - ``true``
     - Optimization on Linux
   * - ``quarkus.http.tcp-fast-open``
     - ``true``
     - Optimization on Linux
   * - ``quarkus.vertx.prefer-native-transport``
     - ``true``
     - Optimization for Various platforms
   * - ``quarkus.console`` related
     -
     - To control if the UI console should be activated or not

The following parameters are for Http service and client.


.. list-table:: Http Quarkus Configuration
   :header-rows: 1

   * - Property/Yaml property
     - Default Value
     - Comment
   * - ``quarkus.http.limits.max-body-size``
     - ``5T``
     - Current limit of Cloud Storage providers
   * - ``quarkus.http.limits.max-chunk-size``
     - ``98304``
     - Best choice between 64K, 98K and 128K; See ``ccs.bufferSize``
   * - ``quarkus.http.limits.max-frame-size``
     - ``98304``
     - Best choice between 64K, 98K and 128K; See ``ccs.bufferSize``
   * - ``quarkus.resteasy-reactive.output-buffer-size``
     - ``98304``
     - Best choice between 64K, 98K and 128K; See ``ccs.bufferSize``
   * - ``quarkus.resteasy-reactive.input-buffer-size``
     - ``98304``
     - Best choice between 64K, 98K and 128K; See ``ccs.bufferSize``
   * - ``quarkus.rest-client.multipart.max-chunk-size``
     - ``98304``
     - Best choice between 64K, 98K and 128K; See ``ccs.bufferSize``
   * - ``quarkus.rest-client.max-chunk-size``
     - ``98304``
     - Best choice between 64K, 98K and 128K; See ``ccs.bufferSize``
   * - ``quarkus.vertx.eventbus.receive-buffer-size``
     - ``98304``
     - Best choice between 64K, 98K and 128K; See ``ccs.bufferSize``
   * - ``quarkus.vertx.eventbus.send-buffer-size``
     - ``98304``
     - Best choice between 64K, 98K and 128K; See ``ccs.bufferSize``
   * - ``quarkus.vertx.warning-exception-time``
     - ``30S``
     - Extending from ``2S``
   * - ``quarkus.vertx.max-event-loop-execute-time``
     - ``30S``
     - Extending from ``2S``

The following parameters are for TlS support.


.. list-table:: TLS Quarkus Configuration
   :header-rows: 1

   * - Property/Yaml property
     - Default Value
     - Comment
   * - ``quarkus.ssl.native``
     - ``true``
     - Allow Native SSL support (OpenSSL)
   * - ``quarkus.http.ssl`` related
     -
     - To handle MTLS
   * - ``quarkus.rest-client.trust-store`` ``quarkus.rest-client.key-store``
     -
     - To handle MTLS
   * - ``quarkus.http.host`` and ``quarkus.http.port/ssl-port``
     -
     - To specify which host and port

The following parameters are for Log and Observability configuration.


.. list-table:: Log Quarkus Configuration
   :header-rows: 1

   * - Property/Yaml property
     - Default Value
     - Comment
   * - ``quarkus.http.access-log`` related
     -
     - To handle Access-log as usual http service
   * - ``quarkus.log.console.format``
     - ``%d{HH:mm:ss,SSS} %-5p [%c{2.}] [%l] (%t) (%X) %s%e%n``
     - To adapt if necessary
   * - ``quarkus.log.console.json`` and related
     -
     - To activate with ``quarkus-logging-json`` module to get log in Json format
   * - ``quarkus.log.level``
     - ``INFO``
     - To adapt as needed
   * - ``quarkus.otel`` related
     -
     - To configure OpenTelemetry for Metrics



.. code-block:: properties
  :caption: Example of http access log configuration

  quarkus.http.access-log.enabled=false
  quarkus.http.record-request-start-time=true
  quarkus.http.access-log.log-to-file=true
  quarkus.http.access-log.base-file-name=quarkus-access-log
  quarkus.http.access-log.pattern=%{REMOTE_HOST} %l %{REMOTE_USER} %{DATE_TIME} "%{REQUEST_LINE}" %{RESPONSE_CODE} %b (%{RESPONSE_TIME} ms) [XOpIdIn: %{i,x-clonecloudstore-op-id} Client: "%{i,user-agent}"] [XOpIdOut: %{o,x-clonecloudstore-op-id} Server: "%{o,server}"] [%{LOCAL_SERVER_NAME}]


The following parameters are for Traffic Shaping (bandwidth control) for Http service.


.. list-table:: Traffic Shaping Quarkus Configuration
   :header-rows: 1

   * - Property/Yaml property
     - Comment
   * - ``quarkus.http.traffic-shaping`` related
     - To enable traffic-shaping if needed (in particular with Replicator)


.. code-block:: properties
  :caption: Example of http traffic-shaping configuration

  quarkus.http.traffic-shaping.enabled=true
  quarkus.http.traffic-shaping.inbound-global-bandwidth=1G
  quarkus.http.traffic-shaping.outbound-global-bandwidth=1G
  quarkus.http.traffic-shaping.max-delay=10s
  quarkus.http.traffic-shaping.check-interval=10s


The following parameters are for Database configuration. Many options exist, and first,
one should decide if MongoDB or PostgreSql is used (see ``ccs.db.type``).


.. list-table:: Database Quarkus Configuration
   :header-rows: 1

   * - Property/Yaml property
     - Default Value
     - Comment
   * - ``quarkus.hibernate-orm`` related
     -
     - For PostgreSql configuration
   * - ``quarkus.hibernate-orm.jdbc.statement-batch-size``
     - ``50``
     - For bulk operation
   * - ``quarkus.hibernate-orm.jdbc.statement-fetch-size``
     - ``1000``
     - For bulk operation
   * - ``quarkus.hibernate-orm.fetch.batch-size``
     - ``1000``
     - For bulk operation
   * - ``quarkus.mongodb`` related
     -
     - For MongoDB configuration

Here are the specific global Clouod Clone Store parameters.

.. list-table:: Common Cloud Clone Store Configuration
   :header-rows: 1

   * - Property/Yaml property
     - Possible Values
     - Default Value
     - Definition
   * - ``ccs.machineId``
     - Hexadecimal format of 6 bytes
     - Empty
     - Internal Machine Id used if specified (not null or empty) using 6 bytes in Hexadecimal format. Should be used in special case where MacAddress is not reliable
   * - ``ccs.bufferSize``
     - Any number of bytes > 8192
     - 96 KB
     - Buffer Size ; Optimal is between 64KB, 96KB and 128KB. Note: Quarkus seems to limit to 64KB but setting the same value gives smaller chunk size
   * - ``ccs.maxWaitMs``
     - Any number of milliseconds (> 100 ms)
     - 1 second
     - Property to define Max waiting time in milliseconds before Time Out within packets (in particular unknown size)
   * - ``ccs.driverMaxChunkSize``
     - Any number > 5M in bytes
     - 512 MB
     - Property to define Buffer Size for a Driver Chunk (may be override by driver specific configuration)
   * - ``ccs.server.computeSha256``
     - Boolean
     - ``false``
     - Property to define if Server will compute SHA 256 on the fly (should be true for Accessor)
   * - ``ccs.client.response.timeout``
     - Any number of milliseconds
     - 6 minutes
     - Property to define Max transferring time in milliseconds before Time Out (must take into account large file and bandwidth)
   * - ``ccs.db.type``
     - mongo or postgre
     - Empty, so Mongo by default
     - Property to define which implementations to use between MongoDB or PostgreSQL
   * - ``ccs.internal.compression``
     - Boolean
     - false
     - Property to define if internal services use ZSTD compression for streams

.. note::
  Note that ZSTD compression is efficient both in cpu and memory while still having a nice compression,
  but if most of the streams are incompressible (such as compressed image, video or ZIP files), it might be better
  to not activate this option. Files in Storage Driver will not be stored compressed whatever (except if
  Cloud Storage compressed them, but this is out of CCS).

Metrics
=========

.. list-table:: Metrics for Cloud Clone Store
   :header-rows: 1

   * - Metric name
     - Tags
     - Definition
   * - ``ccs.drivers3`` or ``ccs.drivergoogle`` or ``ccs.driverazure``
     - ``bucket`` or ``object`` with value ``create``, ``delete``, ``count``, ``stream``, ``exists``, ``read_md``, ``read``, ``copy``,  ``error_(write or read or delete)``
     - Count each category of Driver actions
   * - ``ccs.requestactionconsumer``
     - ``bucket`` or ``object`` with value ``create``, ``delete`` or ``error``
     - Count each category of received Replication Action
   * - ``ccs.localreplicatorrequesttopicconsumer``
     - ``order`` with value ``replicate``
     - Count each category of received Replication Request
   * - ``ccs.buffered_import``
     - ``object`` with value ``create``, ``purge``, ``copy``, ``error_write``, ``register``, ``unregister``
     - Count each category of buffered accessor service using local storage first
   * - ``ccs.purge_service``
     - ``object`` with value ``purge``, ``delete``, ``archive``
     - Count each category of reconciliation process
   * - ``ccs.local_reconciliator``
     - ``object`` with value ``from.db``, ``from.driver``, ``update_from_driver``, ``to.sites_listing``, ``to.remote_site``
     - Count each category of reconciliation process
   * - ``ccs.central_reconciliator``
     - ``site`` with value ``from.remote_site``
     - Count each category of reconciliation process per site
   * - ``ccs.central_reconciliator``
     - ``object`` with value ``from.remote_sites_listing`` or ``to.actions```
     - Count each category of reconciliation process per site
   * - ``ccs.initialization-service``
     - ``object`` with value ``create``
     - Count each category of importing existing Storage Objects process
   * - ``http_server_request_seconds_*``
     - ``uri`` value ``/cloudclonestore/*``
     - Count each category of Public Accessor API call (native metrics)
   * - ``http_server_request_seconds_*``
     - ``uri`` value ``/ccs/internal/*``
     - Count each category of Private Accessor API call (native metrics)
   * - ``http_server_request_seconds_*``
     - ``uri`` value ``/replicator/local/buckets/*``
     - Count each category of Local Replicator API call (native metrics)
   * - ``http_server_request_seconds_*``
     - ``uri`` value ``/replicator/remote/buckets/*``
     - Count each category of Remote Replicator API call (native metrics)
   * - ``http_server_request_seconds_*``
     - ``uri`` value ``/replicator/remote/orders/*``
     - Count each category of Remote Order Replicator API call (native metrics)
   * - ``http_server_request_seconds_*``
     - ``uri`` value ``/replicator/remote/reconciliation/*``
     - Count each category of Remote Reconciliation Replicator API call (native metrics)
   * - ``http_server_request_seconds_*``
     - ``uri`` value ``/reconciliator/*``
     - Count each category of Reconciliator API call (native metrics)
   * - ``http_server_request_seconds_*``
     - ``uri`` value ``/administration/topologies/*``
     - Count each category of Administration (topology) API call (native metrics)
