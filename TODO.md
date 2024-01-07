# Accessor
## listFilter
- Add Option for internal to get true listing from S3
- Might be in Reconciliator directly ?

## Extra module for specific configuration
- Difference by configuration

## Extra API from external to get remote if not exists locally
- Fix API according to definition

# API Security
- Extract standard operation into ApiService
- Create APIs using each OIDC / MTLS extending ApiService
- Document how to deactivate/activate API through application.yml

# Reconciliator
- Create algorithms
  - Purge Objects/Bucket a part, using expires and for delete from available possible archive rule 
  - Error status cleanup
  - Create Request
    - With Filter and Parameters
    - Have API to recv Request
    - Launch local tasks
  - Add Enum SOURCE (DB, S3, MIXED) fromDB into NativeList: Done
  - Compute Local List from Filter (dateFrom/dateTo if any, buckets if any, size if any, Metadata (DB) if any)
    - Get list from DB into Table NativeList
    - Get list from S3 into Table NativeList
    - Get final list comparison between DB and S3 from/to Table NativeList
    - Clean NativeList for DB/S3 according to parameter (TrueClean/KeepPastCheck)
      - Even If Dry-Run
  - Compute fusion list
    - Get final list from remote NativeList to local NativeList
    - Get final list comparison from NativeList to Table SitesList
    - Clean NativeList MIXED according to parameter (as local ones step)
      - If not Dry-run
    - Register remote local / local local list done into Request entry
  - Compute actions when all fusion list done (Request entry)
    - Get final list action from SitesList to SitesAction
    - Compute statistics for Request Table entry
    - Clean SitesList according to parameter (as local one step)
      - If not Dry-run
    - If not dry-run
      - Generate Kafka reconciliation events

# Log
Level:
- DEBUG : OK
- INFO : important info, or else DEBUG

# Build target
- Implement both Mongo or Postgre

# Compression
- Check how to specify compression
  - External Client side ?
    - Sending: Specific option indicating: already compressed / to be compressed / none
    - Receiving: Specific option indicating: accept compress or not (will be always decompressed)
  - Internal client side ? getting from context
    - Sending: 
      - From external client: already or not => compressed whatever
      - From out of external client: compressed whatever
    - Receiving: Accept always compressed 
  - External Server side ?
    - Receiving: set already or not
    - Response: depends on accept compress
