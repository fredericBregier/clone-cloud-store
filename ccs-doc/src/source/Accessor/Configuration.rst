Configuration
*************

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

For instance the steps could be:

- Add this Simple Gateway in front of an application for existing buckets
- In parallel, import the content of those buckets using the special procedure (see Reconcilitator import from existing
  Storage) such that all buckets and associated objects are in the CCS database
- Once import done, the Simple Gateway could be shutdown to let the Accessor Server replacing it, while the application
  still can access to the buckets and associated objects
- When a new site is build, a replication can be setup such that the full services are offer to the
  application (using Reconciliator for instance special procedure for new and empty replication site or
  through standard Reconciliator process for an existing (partial or not) remote site)

*Note that this version has only Ownership control over deletion of a bucket, not any specific control
on other operations such as read, create or delete an object within a bucket.*

Accessor-Server
+++++++++++++++

Used by application (clients) to interact with Cloud Storage, enabling replication and remote access and reconciliation,
using the public API.

It is also used internally by all services when they need to access or interact with buckets and objects, through the
internal API, which must be not available to other services than Cloud Clone Store itself.

*Note that this version has full Ownership control over deletion of a bucket, but also
on other operations such as read, create or delete an object within a bucket from any client.*

Client with Apache httpclient5
=================================

In order to allow more applications to use Clone Cloud Store, an Apache httpclient 5 based CCS client is also
available in **ccs-accessor-client-apache**.

Dependencies is minimal while all functionalities are supported.

Quarkus is not required.

application.yaml configuration
===============================

Client configurations
++++++++++++++++++++++

.. list-table:: Accessor Cloud Clone Store Client Configuration
   :header-rows: 1

   * - Property/Yaml property
     - Possible Values
   * - ``quarkus.rest-client."io.clonecloudstore.accessor.client.api.AccessorBucketApi".url``
     - Http(s) url of the service
   * - ``quarkus.rest-client."io.clonecloudstore.accessor.client.api.AccessorObjectApi".url``
     - Http(s) url of the service


.. list-table:: Accessor Cloud Clone Store Internal Client Configuration
   :header-rows: 1

   * - Property/Yaml property
     - Possible Values
   * - ``quarkus.rest-client."io.clonecloudstore.accessor.client.internal.api.AccessorBucketInternalApi".url``
     - Http(s) url of the service
   * - ``quarkus.rest-client."io.clonecloudstore.accessor.client.internal.api.AccessorObjectInternalApi".url``
     - Http(s) url of the service

Accessor Replicator configuration
++++++++++++++++++++++++++++++++++

.. list-table:: Accessor Replicator Cloud Clone Store Service Configuration
   :header-rows: 1
   :widths: 10 10 5

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
     - Name of the incoming topic for Action Requests (if more than 1 instance, add broadcast=true to the configuration)
     - ``request-action``
   * - ``quarkus.mongodb.database``
     - Name of the associated database (if MongoDB used, with ccs.db.type = mongo)
     -
   * - ``quarkus.rest-client."io.clonecloudstore.replicator.client.api.LocalReplicatorApi".url``
     - Http(s) url of the service
     -
   * - ``quarkus.rest-client."io.clonecloudstore.administration.client.api.OwnershipApi".url``
     - Http(s) url of the service
     -

Accessor configuration
++++++++++++++++++++++


.. list-table:: Accessor Cloud Clone Store Service Configuration
   :header-rows: 1
   :widths: 10 10 7

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
   * - ``quarkus.rest-client."io.clonecloudstore.administration.client.api.OwnershipApi".url``
     - Http(s) url of the service
     -


Accessor Simple Gateway configuration
++++++++++++++++++++++++++++++++++++++++

.. list-table:: Accessor Simple Gateway Cloud Clone Store Service Configuration
   :header-rows: 1

   * - Property/Yaml property
     - Possible Values
     - Default Value
   * - ``ccs.accessor.site``
     - Name of the site
     - ``unconfigured``

Accessor common configuration
++++++++++++++++++++++++++++++

For both *Accessor Replicator Cloud Clone Store Service* and *Accessor Cloud Clone Store Service*,
an extra configuration is needed according to the Storage Driver used:

.. include:: ../driver-api/Configuration.rst


Accessor calls Ownership service to check or create ownership for each bucket.

.. list-table:: Ownership Cloud Clone Store Client Configuration
   :header-rows: 1

   * - Property/Yaml property
     - Possible Values
   * - ``quarkus.rest-client."io.clonecloudstore.administration.client.api.OwnershipApi".url``
     - Http(s) url of the service


Accessor buffered configuration
++++++++++++++++++++++++++++++++

For both *Accessor Simple Gateway Cloud Clone Store Service* and *Accessor Cloud Clone Store Service*,
an extra configuration could be set to allow buffered streams.

.. warning::
  The buffered configuration is intend to protect against non resilient Driver services.
  But it is at the cost of an extra storage on the Accessor service to store temporarily
  the uploaded stream, until they are in the Driver final service.
  **This implies to "guess" how many local storage space is needed.**

This option should not be activated in general, but allows to handle final Driver service that
have a bad resilience, at the price of extra local storage.

The global logic is the following:

- When an upload occurs, the inputStream is first backup into the local filesystem.
- Once backuped, it is then uploaded to the Driver.
- If the Driver upload is in success, the local copy is deleted.
- On the contrary, in case of failure, the local copy remains and is add to an asynchronous retry handler.

  - The related item in database has a READY status, even if not uploaded in Driver service

When an access is tried on a Driver Object, the global logic is the following:

- If the Driver access is in error, there is an extra try using the local filesystem

When a delete occurs on a Driver Object, there is a try to delete also from the local filesystem.

.. warning::
  When having multiple instances of Accessor, if one wants to not rely on which server
  the call occurs, one can share the extra storage (using NFS for instance).
  If not, access and delete will not benefit other instances.

The filesystem is as much as possible guard to prevent no more space on device:

- An option specifies how much space in GB must be available for any upload
- If the upload inputStream size is known, the size is compared to the available space
- In any case where the filesystem is not enough, the local copy will be skipped, therefore
  relying only on the Driver availability

.. warning::
  This protection can prevent buffered operations due to the lack of space.
  In this case, the upload relies only on the Driver availability, which is the
  default behavior.

For any buffered and not yet store to Driver service items, there is an extra backgroung
task that will take them in consideration. The schedule is every ``ccs.accessor.store.schedule.delay``.

- The background task check first if any item shall be uploaded to the Driver

  - It checks the availability in the database (status) and the availability of the Driver
    service and the item is still missing
  - If everything is OK, it uploads the item and then delete locally the item
  - On the contrary

    - If the item is already in the Driver, it is deleted
    - If the item is not in the Driver, it is reset to the next schedule

- Once all scheduled tasks are over

  - It checks too old items and clean them according to the ``purge.retention_seconds``` configuration

    - For each item, the associated entry status in database is placed as ``ERR_UPL```

  - **Important note: once purged, the items cannot be uploaded anymore automatically**


.. warning::
  Most of the normal behaviors of Driver are respected using the buffered space, except
  listing, streaming of StorageObjects and test of existence of a directory (object is checked but
  not directory).


.. list-table:: Buffered upload Cloud Clone Store Service Configuration
   :header-rows: 1

   * - Property/Yaml property
     - Possible Values
     - Default Value
   * - ``ccs.accessor.store.active``
     - true / false
     - false and should be used with caution
   * - ``ccs.accessor.store.path``
     - Path to the root for the local store
     - Temp directory according to io.java.tmpdir (/CCS will be added)
   * - ``ccs.accessor.store.min_space_gb``
     - integer as number of GB
     - 5 GB by default, should be set according to average upload sizes and duration of unavailability of Driver final service
   * - ``ccs.accessor.store.purge.retention_seconds``
     - delay in seconds before purge
     - 3600 seconds (1 hour) by default, should be set according to space on local storage
   * - ``ccs.accessor.store.schedule.delay``
     - delay in duration format ("10s", "1m"...), a number will be considered in seconds by default
     - "10s" (10 seconds) by default, should be set according to space on local storage, upload frequency and Driver service stability

