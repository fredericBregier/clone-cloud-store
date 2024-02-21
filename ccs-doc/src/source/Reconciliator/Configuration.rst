Configuration
*************


.. warning::
  Still in progress

Various Reconciliation services
================================

Remote Listing
++++++++++++++++++++

Local Reconciliation
+++++++++++++++++++++


application.yaml configuration
===============================

.. list-table:: Reconciliator Cloud Clone Store Configuration
   :header-rows: 1

   * - Property/Yaml property
     - Possible Values
     - Default Value
     - Definition
   * - ``ccs.reconciliator.threads``
     - Number of threads to use in certain steps
     - Current number of cores / 2, minimal being 2
     - Used in particular in steps where parallelism can improve efficiency for long term computations
