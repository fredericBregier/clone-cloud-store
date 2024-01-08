Full Build on local
####################################

Use the ``-P benchmark`` to allow to run benchmark tests, place in IT tests.

Note that current implementation changes most of other real IT tests to ITTest tests,
such that they are launched event without this profile ``benchmark``.
The main reasons are:
- Most of those tests are really "simple" IT tests, meaning they are part of Junit tests.
- Aggregation of coverage is easier for those non IT tests

In the same idea, if the CI/CD does support the Sphinx process to build the HTML and PDF documentations, the profile
``-P doc`` can be included.

In order to launch them locally, you have to do the following:

.. code-block:: shell
  :caption: Example for **maven with Doc generation**

  mvn verify
  mvn package -P doc

You can launch only with container (implying all tests plus the one that are using IT name but QuarkusTest,
not QuarkusIntegrationTest), using only **verify** phase.

You can launch only documentation generation, using **package** phase (documentation is build on pre-package phase).

How to integrate Containers in Quarkus tests
==============================================

By default, if any dependencies use containers for testing (Quarkus Dev Support), it will be launch on each and every
tests. Most of the time, that is not an issue, but in some cases we do want to control when a container is
launched or not.

Moreover, if multiple "containers" are defined in the dependencies, they will all be launched for each and every
tests, even if not needed.

So the following is an option to remove those constraints and still being able to launch tests with or without
explicit container(s).

**Of course, if using default Dev services from Quarkus is not an issue, you can still rely on it and therefore ignore
the following.**

Full examples are available within ccs-test-support test sources.

Properties
+++++++++++

Add the following to your application.properties for test (in ``src/test/resources``):

.. code-block:: properties
  :caption: Example for **properties** for Dev Containers

  # Global stop (needed to prevent Ryuk to be launched)
  quarkus.devservices.enabled=false
  # Below according to what is used in the tests
  # Particular stop for S3
  quarkus.s3.devservices.enabled=false
  # Particular stop for database (when not using PostgreSQL but MongoDB, set to true if reversed)
  quarkus.hibernate-orm.enabled=false
  #DO NOT SET THIS: quarkus.hibernate-orm.database.generation = drop-and-create


Handling startup of containers
+++++++++++++++++++++++++++++++

The idea is to launch the container as needed and only when needed.

The following example is for S3.

Use QuarkusTestResourceLifecycleManager and QuarkusTestProfile
---------------------------------------------------------------

``QuarkusTestResourceLifecycleManager`` is intended to provide manual control on resources needed before the Quarkus
test startups. (see Quarkus TESTING YOUR APPLICATION / Starting services before the Quarkus application starts
https://quarkus.io/guides/getting-started-testing#quarkus-test-resource )


2 kinds of ``QuarkusTestResourceLifecycleManager`` can be done.

For no container at all
**************************

.. code-block:: java
  :caption: Example for **NoResource** for no container

  public class NoResource implements QuarkusTestResourceLifecycleManager {
    @Override
    public Map<String, String> start() {
      return SingletonUtils.singletonMap();
    }

    @Override
    public void stop() {
      // Nothing
    }
  }

For a real container
*********************

2 classes are needed, one for the Resource, one for the Container using TestContainers.

The first one defines the Resource to be used and launched before Quarkus starts (mandatory).

.. code-block:: java
  :caption: Example for **MinioResource** for Minio S3 container

  public class MinIoResource implements QuarkusTestResourceLifecycleManager {
    private static final String ACCESS_KEY = "accessKey";
    private static final String SECRET_KEY = "secretKey";
    public static MinioContainer minioContainer =
        new MinioContainer(new MinioContainer.CredentialsProvider(ACCESS_KEY, SECRET_KEY));

    public static String getAccessKey() {
      return minioContainer.getAccessKey();
   }

    public static String getSecretKey() {
      return minioContainer.getSecretKey();
    }

    public static String getUrlString() {
      return minioContainer.getUrlString();
    }

    public static String getRegion() {
      return Regions.EU_WEST_1.name();
    }

    @Override
    public Map<String, String> start() {
      if (!minioContainer.isRunning()) {
        minioContainer.start();
      }
      return minioContainer.getEnvMap();
    }

    @Override
    public void stop() {
      minioContainer.stop();
    }
  }

The second one defines the container to start (using here TestContainers).

.. code-block:: java
  :caption: Example for **MinioContainer** for Minio S3 container

  public class MinioContainer extends GenericContainer<MinioContainer> {
    private static final int DEFAULT_PORT = 9000;
    private static final String DEFAULT_IMAGE = "minio/minio";

    private static final String MINIO_ACCESS_KEY = "MINIO_ACCESS_KEY";
    private static final String MINIO_SECRET_KEY = "MINIO_SECRET_KEY";

    private static final String DEFAULT_STORAGE_DIRECTORY = "/data";
    private static final String HEALTH_ENDPOINT = "/minio/health/ready";

    public MinioContainer(final CredentialsProvider credentials) {
      this(DEFAULT_IMAGE, credentials);
    }

    public MinioContainer(final String image, final CredentialsProvider credentials) {
      super(image == null ? DEFAULT_IMAGE : image);
      withNetworkAliases("minio-" + Base58.randomString(6));
      withExposedPorts(DEFAULT_PORT);
      if (credentials != null) {
        withEnv(MINIO_ACCESS_KEY, credentials.getAccessKey());
        withEnv(MINIO_SECRET_KEY, credentials.getSecretKey());
      }
      withCommand("server", DEFAULT_STORAGE_DIRECTORY);
      setWaitStrategy(new HttpWaitStrategy().forPort(DEFAULT_PORT).forPath(HEALTH_ENDPOINT)
          .withStartupTimeout(Duration.ofMinutes(2)));
    }

    public URL getURL() throws MalformedURLException {
      return new URL(getUrlString());
    }

    public String getUrlString() {
      return "http://" + getHost() + ":" + getMappedPort(DEFAULT_PORT);
    }

    public String getAccessKey() {
      return getEnvMap().get(MINIO_ACCESS_KEY);
    }

    public String getSecretKey() {
      return getEnvMap().get(MINIO_SECRET_KEY);
    }

    public static class CredentialsProvider {
      private final String accessKey;
      private final String secretKey;

      public CredentialsProvider(final String accessKey, final String secretKey) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
      }

      public String getAccessKey() {
        return accessKey;
      }

      public String getSecretKey() {
        return secretKey;
      }
    }
  }


QuarkusTestProfile
**************************

Once build, the recommended way is to use a QuarkusTestProfile.


.. code-block:: java
  :caption: Example for **NoResourceProfile** for no Dev services

  public class NoResourceProfile  implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(ResourcesConstants.QUARKUS_DEVSERVICES_ENABLED, "false");
    }

    @Override
    public boolean disableGlobalTestResources() {
      return true;
    }

    @Override
    public String getConfigProfile() {
      return "test-noresource";
    }
  }

.. code-block:: java
  :caption: Example for **MinioProfile** for Minio S3 container

  public class MinioProfile  implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(ResourcesConstants.QUARKUS_DEVSERVICES_ENABLED, "false");
    }

    @Override
    public boolean disableGlobalTestResources() {
      return true;
    }

    @Override
    public String getConfigProfile() {
      return "test-minio";
    }

    @Override
    public List<TestResourceEntry> testResources() {
      return Collections.singletonList(new TestResourceEntry(MinIoResource.class));
    }
  }

**Special attention on Database support**: In order to be able to choose between PostgreSql implementation or MongoDB
implementation at runtime, the following properties are needed additionally:

.. code-block:: java
  :caption: Additional properties for PostgreSql/MongoDb support at runtime

  @Override
  public Map<String, String> getConfigOverrides() {
    return Map.of(ResourcesConstants.QUARKUS_DEVSERVICES_ENABLED, "false",
        // Specify false for Mnngo, True for Postgre
        ResourcesConstants.QUARKUS_HIBERNATE_ORM_ENABLED, "false",
        // Specify MONGO for Mnngo, POSTGRE for Postgre
        ResourcesConstants.CCS_DB_TYPE, ResourcesConstants.MONGO);
  }

In Production configuration, the same 2 properties are to be setup:

.. code-block:: properties
  :caption: Additional properties for PostgreSql/MongoDb support at runtime

  quarkus.hibernate-orm.enabled= false / true
  ccs.db.type=mongo / postgre

In the test classes
*******************

The 2 first examples are about testing in Test mode (not IT) without any container launched.

First example is about using no Container in the test: Class Test name can be without IT but as ``XxxTest``.

.. code-block:: java
  :caption: Example of usage of **NoResource** for No container in a test

  // Do not use @QuarkusIntegrationTest
  @QuarkusTest
  @TestProfile(NoResourceProfile.class)
  public class DriverS3NoS3ConfiguredTest {

  }

Second example is the same, without any container, but without using the **NoResourceProfile.class**. This will probably
generate some warn log about non available services, but they should not be harmful.

.. code-block:: java
  :caption: Example without usage of **NoResource** in a test

  // Do not use @QuarkusIntegrationTest
  @QuarkusTest
  public class DriverS3NoS3ConfiguredTest {

  }

Third example is about using a Container in the test: Class Test name must end with IT as ``XxxIT``.

.. code-block:: java
  :caption: Example of usage of **MinioProfile** for Minio S3 container in a test

  // Do not use @QuarkusIntegrationTest
  @QuarkusTest
  // Define Minio Profile
  @TestProfile(MinioProfile.class)
  public class DriverS3MinioIT extends DriverS3Base {

    @BeforeAll
    static void setup() {
      // Example: usage of MinioResource to setup the parameters that should be loaded from properties in normal code
      StgDriverS3Properties.setDynamicS3Parameters(MinIoResource.getUrlString(), MinIoResource.getAccessKey(),
          MinIoResource.getSecretKey(), MinIoResource.getRegion());
    }
  }

