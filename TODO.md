# Accessor

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
- Create APIs
- Eventual limited IHM on Admsinitration

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
  - Config of Reconciliation
