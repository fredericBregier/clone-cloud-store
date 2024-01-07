/*
 * Copyright (c) 2022-2024. Clone Cloud Store (CCS), Contributors and Frederic Bregier
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed
 *  under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
 *  OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.clonecloudstore.common.database.mongo;

import java.util.stream.Stream;

import com.mongodb.client.model.Filters;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.DbUpdate;
import io.clonecloudstore.common.database.utils.RepositoryBaseInterface;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.standard.stream.ClosingIterator;
import io.quarkus.mongodb.panache.PanacheMongoRepositoryBase;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.EncoderContext;

/**
 * The base Mongo implementation of the RepositoryBaseInterface
 *
 * @param <E> the type for the DTO to use
 */
public abstract class ExtendedPanacheMongoRepositoryBase<F, E extends F>
    implements PanacheMongoRepositoryBase<E, String>, RepositoryBaseInterface<F> {
  protected final MongoBulkInsertHelper<F, E> helper = new MongoBulkInsertHelper<>();
  protected final MongoStreamHelper<F, E> streamHelper = new MongoStreamHelper<>();

  @Override
  public boolean isSqlRepository() {
    return false;
  }

  @Override
  public ExtendedPanacheMongoRepositoryBase<F, E> addToInsertBulk(final F so) throws CcsDbException {
    if (helper.addToInsertBulk((E) so)) {
      flushAll();
    }
    return this;
  }

  private Document getDocumentFromObject(final F update) {
    BsonDocument unwrapped = new BsonDocument();
    BsonWriter jsonWriter = new BsonDocumentWriter(unwrapped);
    final var encoder = (Codec<E>) this.mongoCollection().getCodecRegistry().get(update.getClass());
    encoder.encode(jsonWriter, (E) update, EncoderContext.builder().build());
    final var json = unwrapped.toJson();
    return Document.parse(json);
  }

  public ExtendedPanacheMongoRepositoryBase<F, E> addToUpsertBulk(final Document find, final F update)
      throws CcsDbException {
    try {
      final var document = getDocumentFromObject(update);
      if (helper.addToUpsertBulk(find, document, (E) update)) {
        flushAll();
      }
    } catch (final RuntimeException e) {
      throw new CcsDbException("Issue with Serialization", e);
    }
    return this;
  }

  public ExtendedPanacheMongoRepositoryBase<F, E> addToUpdateBulk(final Document find, final F update)
      throws CcsDbException {
    try {
      final var document = getDocumentFromObject(update);
      if (helper.addToUpsertBulk(find, document, null)) {
        flushAll();
      }
    } catch (final RuntimeException e) {
      throw new CcsDbException("Issue with Serialization", e);
    }
    return this;
  }

  public record ImmutablePair<E>(Document find, E update) {
    // Empty
  }

  @Override
  public void flushAll() throws CcsDbException {
    helper.bulkPersist((RepositoryBaseInterface<E>) this);
    helper.bulkUpsert((RepositoryBaseInterface<E>) this);
  }

  @Override
  public F findOne(final DbQuery query) throws CcsDbException {
    try {
      if (query.isEmpty()) {
        return mongoCollection().find().first();
      }
      return mongoCollection().find(query.getBson()).first();
    } catch (final RuntimeException e) {
      throw new CcsDbException("FindOne in error", e);
    }
  }

  @Override
  public F findWithPk(final String pk) throws CcsDbException {
    try {
      return mongoCollection().find(Filters.eq(ID, pk)).first();
    } catch (final RuntimeException e) {
      throw new CcsDbException("FindWitPk in error", e);
    }
  }

  @Override
  public void deleteWithPk(final String pk) throws CcsDbException {
    try {
      mongoCollection().deleteOne(Filters.eq(ID, pk));
    } catch (final RuntimeException e) {
      throw new CcsDbException("deleteWithPk in error", e);
    }
  }

  @Override
  public Stream<F> findStream(final DbQuery query) throws CcsDbException {
    try {
      return (Stream<F>) streamHelper.findStream((RepositoryBaseInterface<E>) this, query);
    } catch (final RuntimeException e) {
      throw new CcsDbException("findStream in error", e);
    }
  }

  @Override
  public ClosingIterator<F> findIterator(final DbQuery query) throws CcsDbException {
    try {
      return (ClosingIterator<F>) streamHelper.findIterator((RepositoryBaseInterface<E>) this, query);
    } catch (final RuntimeException e) {
      throw new CcsDbException("findIterator in error", e);
    }
  }

  @Override
  public long count(final DbQuery query) throws CcsDbException {
    try {
      if (query.isEmpty()) {
        return count();
      }
      return count(query.getBson().toBsonDocument().toJson());
    } catch (final RuntimeException e) {
      throw new CcsDbException("count in error", e);
    }
  }

  @Override
  public long countAll() throws CcsDbException {
    try {
      return this.count();
    } catch (final RuntimeException e) {
      throw new CcsDbException("countAll in error", e);
    }
  }

  @Override
  public long deleteAllDb() throws CcsDbException {
    try {
      return this.deleteAll();
    } catch (final RuntimeException e) {
      throw new CcsDbException("deleteAllDb in error", e);
    }
  }

  @Override
  public long delete(final DbQuery query) throws CcsDbException {
    try {
      if (query.isEmpty()) {
        return deleteAll();
      }
      return delete(query.getBson().toBsonDocument().toJson());
    } catch (final RuntimeException e) {
      throw new CcsDbException("delete in error", e);
    }
  }

  @Override
  public long update(final DbQuery query, final DbUpdate update) throws CcsDbException {
    try {
      if (query.isEmpty()) {
        return mongoCollection().updateMany(Filters.empty(), update.getBson()).getMatchedCount();
      }
      return mongoCollection().updateMany(query.getBson(), update.getBson()).getMatchedCount();
    } catch (final RuntimeException e) {
      throw new CcsDbException("update in error", e);
    }
  }

  @Override
  public boolean updateFull(final F item) throws CcsDbException {
    try {
      PanacheMongoRepositoryBase.super.update((E) item);
      return true;
    } catch (final RuntimeException e) {
      throw new CcsDbException("updateFull in error", e);
    }
  }

  @Override
  public void insert(final F item) throws CcsDbException {
    try {
      persist((E) item);
    } catch (final RuntimeException e) {
      throw new CcsDbException("insert in error", e);
    }
  }
}
