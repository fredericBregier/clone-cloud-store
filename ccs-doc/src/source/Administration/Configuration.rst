Configuration
*************

** TODO **

Various Administration services
================================

Topology
++++++++++++++++++++



application.yaml configuration
===============================


.. list-table:: Topology Cloud Clone Store Client Configuration
   :header-rows: 1

   * - Property/Yaml property
     - Possible Values
     - Default Value
   * - ``quarkus.rest-client."io.clonecloudstore.topology.client.api.TopologyApi".url``
     - Http(s) url of the service
     -



.. list-table:: Topology Cloud Clone Store Service Configuration
   :header-rows: 1

   * - Property/Yaml property
     - Possible Values
     - Default Value
   * - ``ccs.accessor.site``
     - Name of the site
     - ``unconfigured``
   * - ``quarkus.mongodb.database``
     - Name of the associated database (if MongoDB used, with ccs.db.type = mongo)
     -
