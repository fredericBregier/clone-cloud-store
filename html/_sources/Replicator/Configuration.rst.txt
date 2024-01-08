Configuration
*************

** TODO **

Various Replicator services
================================

Both services run in the same server.

Local Replicator
++++++++++++++++++++

It's role is to be contacted by local services to interact with remote services.
- Through API for remote access, it proxies the request from Accessor Services (Accessor Public Service or Accessor Replicator Service) to the remote Replicator (remote site).
- Through topic for Replication Requests, which are sent to remote replicator service through API.

So its role is to handle outgoing requests.

Remote Replicator
+++++++++++++++++++++

It's role is to handle remote requests:
- Remote access through API goes to Accessor Internal Local service
- Remote Replication Request are pushed into topic for Replication Actions. Those are handle then by the Accessor Replicator Local service.

So its role is to handle incoming requests.

application.yaml configuration
===============================

.. list-table:: Replicator Cloud Clone Store Client Configuration
   :header-rows: 1

   * - Property/Yaml property or Environment variable
     - Possible Values
     - Default Value
   * - ``quarkus.rest-client."io.clonecloudstore.replicator.client.api.LocalReplicatorApi".url``
     - Http(s) url of the service
     -
   * - Redefining ``mp.messaging.outgoing.replicator-request-out`` or env ``CCS_REQUEST_REPLICATION``
     - Name of the outgoing topic for Replication Requests
     - ``request-replication``


.. list-table:: Replicator Cloud Clone Store Service Configuration
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
   * - Redefining ``mp.messaging.outgoing.replicator-action-out`` or env ``CCS_REQUEST_ACTION``
     - Name of the outgoing topic for Action Requests
     - ``request-action``
   * - Redefining ``mp.messaging.outgoing.replicator-request-out`` or env ``CCS_REQUEST_REPLICATION``
     - Name of the incoming and outgoing topic for Replication Requests
     - ``request-replication``
   * - ``quarkus.rest-client."io.clonecloudstore.accessor.client.internal.api.AccessorBucketInternalApi".url``
     - Http(s) url of the service
     -
   * - ``quarkus.rest-client."io.clonecloudstore.accessor.client.internal.api.AccessorObjectInternalApi".url``
     - Http(s) url of the service
     -
   * - ``quarkus.rest-client."io.clonecloudstore.replicator.server.remote.client.api.RemoteReplicatorApi".url``
     - Http(s) url of the remote service
     -
   * - ``quarkus.rest-client."io.clonecloudstore.topology.client.api.TopologyApi".url``
     - Http(s) url of the service
     -


