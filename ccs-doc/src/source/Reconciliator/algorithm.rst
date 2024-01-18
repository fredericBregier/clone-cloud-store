Reconciliator's Algorithm
*************************

.. warning::
  Still in progress

Recurrent purge
==================

From time to time, the recurrent purge is there to clean up the database:

- Purge a "Deleted" object (really deleted) once the "expiry" date is over
- Once the object reached its expiry date:

  - Possibly moving it to a dedicated Bucket for archiving with possible new Expiry date

    - If archival is enabled, only one bucket is dedicated to archival process per client
    - Archival is only valid for object with a valid Driver Object

  - Or run "Delete" operation, setting a new "Expiry" date for later on purge
  - In all case, send a message to replicate the deletion order to other sites


Those actions should not be done during reconciliation process but at least before.

.. csv-table:: Recurrent Purge on Expired date
   :file: files/algorithm-purge-expired.csv
   :align: center
   :widths: auto
   :header-rows: 1

Reconciliation
================

Local Reconciliation is in several steps:

- Clean step on Objects and Native local objects status according to Request filter
- Start a new Native local objects for Request, from previous run if any
- Snapshot: Fill or Update Native local objects according to Request filter using Objects database
- Snapshot: Fill or Update Native local objects according to Request filter using Driver contents (probable longest duration)
- Create Site Listing from Native local objects and update if necessary Objects accordingly
- Get Site Listing for remote Reconciliation site, the one that starts this reconciliation process

Optional steps for Local Reconciliation:

- Clean Native local objects if not to be kept for later use
- Once validate remotely, Clean local Site Listing if not to be kept for later use

On site where this process is started:

- Create the Request context
- Send order of local reconciliation process on each remote site and itself
- For each remote site, get the remote Site Listing to append to the local therefore global Site Listing
- Final Reconciliation step

Final Reconciliation steps:

- For all entries in Global Site Listing:

  - Compute Site Action for All sites / Partial sites context

- For all Site Actions, transfer replication order accordingly to each site, even itself

  - Sites having READY ACTION will be considered as remote source site for other sites

- Once all transferred, clean Site Actions
- Statistics update using previous steps

Then reconciliation corrections happen using standard replication order, using special status to act according to the needs:

- DELETE ACTION: will delete if possible MD and Driver or both
- UPDATE ACTION: will update Object only from remote site, Driver being ready
- UPLOAD ACTION: will update Driver (content) and possibly Object (some metadata) from remote site

Clean step
++++++++++

Clean step is several sub-steps: on Objects and Native local objects status according to Request filter

- All Unknown status Objects and Native local objects are removed
- Update Objects reconciliation status from UPLOAD/ERR_UPL to TO_UPDATE
- Update Objects reconciliation status from DELETING/ERR_DEL to DELETING
- Update Objects reconciliation status from READY or DELETED to respectively the same
- Purge Native local objects with status UNKNOWN, UPLOAD, ERR_UPL, DELETED or ERR_DEL

.. csv-table:: Pre Reconciliation Purge
   :file: files/algorithm-purge.csv
   :align: center
   :widths: auto
   :header-rows: 1

.. csv-table:: Pre Result Reconciliation Purge
   :file: files/algorithm-previous-results-purge.csv
   :align: center
   :widths: auto
   :header-rows: 1

Snapshot step
++++++++++++++

Two steps are concerned:

- Fill or Update Native local objects according to Request filter using Objects database

  - If the reconciliation status is a "delete" status, the driver part is ignored

- Fill or Update Native local objects according to Request filter using Driver contents (probable longest duration)

  - Will add or update the Driver part

.. csv-table:: Load from DB and Driver
   :file: files/algorithm-from-db-from-driver.csv
   :align: center
   :widths: auto
   :header-rows: 1

Local Reconciliation step
++++++++++++++++++++++++++++

Create Site Listing from Native local objects and update if necessary Objects accordingly:

- From Driver only, consider Object shall be READY and To Update

  - Create missing Object with existing metadata from Driver (possibly some missing)

- From Db only, consider Delete like as Deleted, and others (Object shall exist) as To Upload again

  - Update Objects accordingly

- From both, consider Delete like as To Delete, and others (Object present but not ready except READY ones) as To Update (metadata only)

  - Update Objects accordingly

.. csv-table:: Fix LocalSite Reconciliation: Driver present, DB absent
   :file: files/algorithm-fix-local-no-db.csv
   :align: center
   :widths: auto
   :header-rows: 1

Once done, the to update ones will be update from the Driver and set as Ready.

.. csv-table:: Fix LocalSite Reconciliation: DB present, Driver absent with Available like status
   :file: files/algorithm-fix-local-no-driver-write.csv
   :align: center
   :widths: auto
   :header-rows: 1

.. csv-table:: Fix LocalSite Reconciliation: DB present, Driver absent with Delete like status
   :file: files/algorithm-fix-local-no-driver-delete.csv
   :align: center
   :widths: auto
   :header-rows: 1

.. csv-table:: Fix LocalSite Reconciliation: DB and Driver presents with Ready like status
   :file: files/algorithm-local-db-driver-write.csv
   :align: center
   :widths: auto
   :header-rows: 1

Once done, the to update ones will be update from the Driver and set as Ready.


.. csv-table:: Fix LocalSite Site Reconciliation: DB and Driver with Delete like status
   :file: files/algorithm-local-db-driver-delete.csv
   :align: center
   :widths: auto
   :header-rows: 1


Final Reconciliation step
+++++++++++++++++++++++++

From all remote Reconciliation site listing, Actions are sorted according to descending event dates, the latest being
the primary event.

Thr order of actions is: DELETE > READY > UPDATE > UPLOAD

So for instance:

- latest event: DELETE like and anything else

  - => DELETE everywhere

- latest event: READY like (UPDATE/UPLOAD)

  - => UPDATE or UPLOAD from READY site(s) (potentially multiples sources)
  - Special case: if none are READY, UPDATE ones will changed to READY

  - Special case latest event: all UPLOAD status (no READY or UPDATE)

    - These final cases are in big trouble since there is no more available correct information

      - UPLOAD cannot be fixed if there is no source at all => changed to ERROR_ACTION with no source to get ERR_UPL status

Two cases have to be checked: all sites or subset of sites are referenced for each item:

- One entry has all sites referenced: so all know about it
- One entry has a subset of all sites referenced: therefore, except for delete action where they are ignored,
  they should be considered as an UPLOAD action (for UPDATE, the concerned site will upgrade locally to UPLOAD since
  no object present)

Those 2 cases are fusion in one:

- For all Site Actions, transfer replication order accordingly to each site, even itself

  - Sites having READY/UPDATE ACTION will be considered as remote source site for other sites


.. csv-table:: Compute Remote Site Action Reconciliation
   :file: files/algorithm-local-to-remote-rules.csv
   :align: center
   :widths: auto
   :header-rows: 1


.. warning::
  Transfer replication order and application not yet implemented

.. csv-table:: Remote Site Action final Reconciliation
   :file: files/algorithm-remote-to-action.csv
   :align: center
   :widths: auto
   :header-rows: 1


Special Reconciliation modes
=============================

Two special cases are implemented:

- Initialization from existing object in Driver Storage while CCS was not yet used to create them
- Initialization for a new site (whatever really new one or disaster one so almost new), in order to speed up
  reconciliation step for this new site from an existing site

Initialization from existing Object Storage without CCS
++++++++++++++++++++++++++++++++++++++++++++++++++++++++

When moving an existing application with existing Objects to Cloud Clone Store, one could use the following batch:

- From Storage Driver, initialize Objects and Buckets in database according to arguments

  - Arguments such as: bucket name, client Id to use, common specific metadata

Note that the issue right now identified is that Bucket are named using clientId within CCS.
To enable such an import, a special attention should be done on this case (where bucket does not have ClientId in
its final name).

All items will have READY status.

PRA reinitialization or new site initialization
++++++++++++++++++++++++++++++++++++++++++++++++

When a site has a disaster (partial or full disaster) or when a new site is added to an existing multi-sites CCS
configuration, there is a special batch to resume the CCS database and Cloud Storage contents.

Once the CCS is installed (or reinstalled), instead of running a standard Reconciliation, one can run this specific
Reconciliation from existing (or none) status on the new/rebuild site.

- Mode empty site: no objets neither storage objects in the site to synchronize

  - This mode is optimize for "all" synchro mode with no control on destination site since nothing is there
  - ALl items will have READY Status using UPLOAD_ACTION from given existing sites

- Mode disaster recovery: objects or storage objects can exist, partially

  - This mode is optimize for "all" synchro mode with control on destination site since objects or storage objects or
    both can exist
  - ALl items will have READY Status using UPGRADE_ACTION from given existing sites

