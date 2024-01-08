Missing or In Progress Functionalities
***************************************

- API could change, in particular Accessor public API (for client application) (see Client Authentication)

- Client without Quarkus is still on going

  - The idea is to allow non Quarkus application in Java to have a ready client SDK.
  - For Quarkus application, the client already exists

- Client authentication

  - Could be done through MTLS or OIDC
  - For CCS interservice authentication, MTLS is the choice but not yet implemented
  - Note that API, in particular public API of Accessor Service, could change due to choice of Authentication;
    For instance, currently, the clientId is passed as a header but later on could be deduced from authentication

- Reconciliation

  - First steps on Reconciliation computations are still in progress
  - Note that replication is active and remote access if not locally present is possible (through configuration)

- PostgreSQL full support

  - Currently, only MongoDB is fully supported.
  - PostgreSQL shall be available soon.
  - Missing Liquibase configuration for both PostgreSql and MongoDB

- Kafka is the default Topic manager. However, switching to Apache Pulsar should be easy by just applying
  changes to pom.xml (moving from Kafka to Pulsar) and to application.yaml to ensure correct configuration is done.

- Advanced functionalities such as:

  - Allowing specific access on all or part of CRUD options to a Bucket owned by an application to another one
    (for instance, to allow producer / consumer of files)
  - Compression of HTTPS link is functional but not yet activated (and will be based on a property)
  - Bandwidth limitation is moved to Quarkus normal configuration (see https://quarkus.io/guides/http-reference#configure-traffic-shaping)

    - It shall be useful only for Replicator and in particular in outbound global mode per site

  - Quarkus Metrics are available but not yet for actions within Clone Cloud Store. The work is on going.
  - Health check service to be done

- Distribution of final jars according to various options is still in debate

  - A choice between Kafka or Pulsar implies 2 different jar due to pom differences
  - However, for PostgreSql or MongoDB, it can be done through configuration so keeping one jar
  - Should it be separate jar (individual per module and per option) or flatten jar (per option)?
