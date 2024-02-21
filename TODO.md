# Accessor
## Bucket to not differ between internal and external
- Allows to "import" existing bucket

## listFilter
- Add Option for internal to get true listing from S3
- Might be in Reconciliator directly ?

## buffered content
- With special option, allow to store locally for multiple tries to store to Driver
  - Probably: no database until valid
  - Could be done through local replicator topic with special order (path to content and metadata files)
  - Option to let replication to be done however to limit impact (with replication remote access from file not Driver)

# API Security
- Extract standard operation into ApiService
- Create APIs using each OIDC / MTLS extending ApiService
- Document how to deactivate/activate API through application.yml

# Reconciliator
- Create algorithms
  - Compute actions when all fusion list done (Request entry)
    - Get final list action from SitesList to SitesAction
    - Compute statistics for Request Table entry
    - Clean SitesList according to parameter (as local one step)
      - If not Dry-run
    - If not dry-run
      - Generate Kafka reconciliation events
  - Compute actions for new site or crashed site
  - Initialization from existing Driver content before CCS

# Build
- Implement both Mongo or Postgre
  - Add Postgre implementation
- Check using Pulsar
- Minimal dockerfiles
- Healthcheck to complete

# Access management
- Extra module for administration configuration
  - Difference by configuration for Oracle/Postgre
  - Config of topology per bucket
  - Config of ownership and delegation
  - Config of replication proactive
  - Config of archiving
  - Config of buffering (write mode)
