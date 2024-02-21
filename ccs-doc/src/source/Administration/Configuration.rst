Configuration
*************


.. warning::
  Still in progress

Various Administration services
================================

Topology
++++++++++++++++++++

This service contains the topology of related Cloud Clone Store sites that are connected.

Right now, all existing declared sites (and active) are considered as part of the replication set for
all buckets.

Later on, will improve this to allow replication set by buckets, such that for instance one bucket
could have no linked remote site, while another one can, and not necessary all or the same than a third bucket.

Ownership
++++++++++++++++++++

Ownership defines right to READ, WRITE or DELETE into a bucket for a client.

This allows to share bucket between clients, with the needed rights.

application.yaml configuration
===============================


.. list-table:: Topology Cloud Clone Store Client Configuration
   :header-rows: 1

   * - Property/Yaml property
     - Possible Values
   * - ``quarkus.rest-client."io.clonecloudstore.administration.client.api.TopologyApi".url``
     - Http(s) url of the service

.. list-table:: Ownership Cloud Clone Store Client Configuration
   :header-rows: 1

   * - Property/Yaml property
     - Possible Values
   * - ``quarkus.rest-client."io.clonecloudstore.administration.client.api.OwnershipApi".url``
     - Http(s) url of the service
