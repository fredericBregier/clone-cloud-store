Common DB
###############

DB Utils
**********

RestQuery, DbQuery and DbUpdate
+++++++++++++++++++++++++++++++

**RestQuery** allows to define "standard" query in a Object model, in order to be able to serialize into a Json.
This Json can then be sent through REST API.

It focuses on the "Where" condition only and therefore can be used for any SELECT, INSERT or UPDATE command.

**DbQuery** allows to generate a SQL (PostgreSQL) or NoSQL (MongoDB) query. It can be used to express a request
and using the Repository model, it will be taken into account natively, for both model (SQL or NoSQL).

It focuses on the "Where" condition only and therefore can be used for any SELECT, INSERT or UPDATE command.

**DbUpdate** alows to general e SQL (PostgreSQL) or NoSQL (MongoDB) Update part query. It can be used to express the
Update part and using the Repository model, it will be taken into account natively, for both model (SQL or NoSQL).

It focuses on the "Update" part (SET) condition only and therefore can be used for UPDATE command only, in conjunction
of a *DbQuery*.

StreamHelperAbstract
++++++++++++++++++++++++

**StreamHelperAbstract** allows to handle easily Stream (real Stream) on SELECT.
It allows to limit memory usage.

RepositoryBaseInterface
+++++++++++++++++++++++

**RepositoryBaseInterface** allows to specify common methods for all repositories, whatever Sql or NoSQL.


.. code-block:: java
  :caption: Example code for **Global Model definition**

  @MappedSuperclass
  public abstract class DbDtoExample {
    // No Id nor BsonId
    // Here come other fields

    @Transient
    public void fromTransferRequest(DtoExample dto) {
      setGuid(dto.getGuid()); // and other setters
    }

    @Transient
    public DtoExample getTransferRequest() {
      DtoExample transferRequest = new DtoExample();
      transferRequest.setGuid(getGuid()); // and other setters
      return transferRequest;
    }

    public abstract String getGuid();

    public abstract DbDtoExample setGuid(String guid);
  }
  public interface DbDtoExampleRepository extends RepositoryBaseInterface<DbDtoExample> {
    String TABLE_NAME = "dto_example";
    // Here other field names
  }

.. code-block:: java
  :caption: Example code for **Global DTO definition**

  @RegisterForReflection
  public class DtoExample extends DbDtoExample {
    private String guid;

    public String getGuid() {
      return guid;
    }

    public DtoExample setGuid(final String guid) {
      this.guid = guid;
      return this;
    }
  }


MongoDb
********

It provides the implementations for all DB-Utils package for NoSQL MongoDB.

.. code-block:: java
  :caption: Example code for **MongoDB Model Implementation definition**

  @MongoEntity(collection = TABLE_NAME)
  public class MgDbDtoExample extends DbDtoExample {
    @BsonId
    private String guid;

    public MgDbDtoExample() {
      // Empty
    }

    public MgDbDtoExample(final DtoExample dto) {
      fromTransferRequest(dto);
    }

    @Override
    public String getGuid() {
      return guid;
    }

    @Override
    public MgDbDtoExample setGuid(final String guid) {
      this.guid = guid;
      return this;
    }
  }

  @ApplicationScoped
  public class MgDbDtoExampleRepository extends ExtendedPanacheMongoRepositoryBase<DbDtoExample, MgDbDtoExample>
      implements DbDtoExampleRepository {
    @Override
    public String getTable() {
      return TABLE_NAME;
    }
  }


In addition, it provides **MongoSqlHelper** to help to build SQL request from DbQuery and DbUpdate.

It provides also an abstraction **AbstractCodec** to make easier the declaration of Codec for DTO (see example).

.. code-block:: java
  :caption: Example code for **AbstractCodec**

  public class MgDbDtoExampleCodec extends AbstractCodec<MgDbDtoExample> {
    public MgDbDtoExampleCodec() {
      super();
    }

    @Override
    protected void setGuid(final MgDbDtoExample mgDbDtoExample, final String guid) {
      mgDbDtoExample.setGuid(guid);
    }

    @Override
    protected String getGuid(final MgDbDtoExample mgDbDtoExample) {
      return mgDbDtoExample.getGuid();
    }

    @Override
    protected MgDbDtoExample fromDocument(final Document document) {
      MgDbDtoExample mgDbDtoExample = new MgDbDtoExample();
      mgDbDtoExample.setField1(document.getString(FIELD1));
      mgDbDtoExample.setField2(document.getString(FIELD2));
      mgDbDtoExample.setTimeField(document.get(TIME_FIELD, Date.class).toInstant());
      return mgDbDtoExample;
    }

    @Override
    protected void toDocument(final MgDbDtoExample mgDbDtoExample, final Document document) {
      document.put(FIELD1, mgDbDtoExample.getField1());
      document.put(FIELD2, mgDbDtoExample.getField2());
      document.put(TIME_FIELD, mgDbDtoExample.getTimeField());
    }

    @Override
    public Class<MgDbDtoExample> getEncoderClass() {
      return MgDbDtoExample.class;
    }
  }

MongoBulkInsertHelper
+++++++++++++++++++++

**MongoBulkInsertHelper** allows to handle easily bulk operation on INSERT or UPDATE for MongoDB.

PostgreSQL
***********

It provides the implementations for all DB-Utils package for SQL PostgreSQL.


.. code-block:: java
  :caption: Example code for **PostgreSQL Model Implementation definition**

  @Entity
  @Table(name = TABLE_NAME,
      indexes = {@Index(name = TABLE_NAME + "_filter_idx", columnList = FIELD1 + ", " + TIME_FIELD)})
  public class PgDbDtoExample extends DbDtoExample {
    @Id
    @Column(name = ID, nullable = false, length = 40)
    private String guid;

    public PgDbDtoExample() {
      // Empty
    }

    public PgDbDtoExample(final DtoExample dto) {
      fromDto(dto);
    }

    @Override
    public String getGuid() {
      return guid;
    }

    @Override
    public PgDbDtoExample setGuid(final String guid) {
      this.guid = guid;
      return this;
    }
  }

  @ApplicationScoped
  @Transactional
  public class PgDbDtoExampleRepository extends ExtendedPanacheRepositoryBase<DbDtoExample, PgDbDtoExample>
      implements DbDtoExampleRepository {
    public PgDbDtoExampleRepository() {
      super(new PgDbDtoExample());
    }

    @Override
    public String getTable() {
      return TABLE_NAME;
    }
  }


In addition, it provides **PostgreSqlHelper** to help to build SQL request from DbQuery and DbUpdate.

It provides also 2 extra Types supported by PostgreSQL:

- Set Type as an Array implementation (**PostgreStringArrayType**)

  - Set to Array shall be implemented carefully within the DTO class (see example)

- Map Type (String, String) as a Jsonb implementation (**PostgreStringMapAsJsonbType**)

.. code-block:: java
  :caption: Example code for **PostgreStringArrayType** and **PostgreStringMapAsJsonbType**

  @Column(columnDefinition = "text[]", name = ARRAY1)
  @Type(type = ARRAY_TYPE_CLASS)
  private String[] array1;
  /**
   * To get a Set internally instead of an array
   */
  @IgnoreProperty
  @Transient
  private final Set<String> set1 = new HashSet<>();
  /**
   * To ensure array ans set are correctly initialized
   */
  @IgnoreProperty
  @Transient
  private boolean checked;
  @Column(name = MAP1, columnDefinition = JSON_TYPE)
  @Type(type = MAP_TYPE_CLASS)
  private final Map<String, String> map1 = new HashMap<>();

