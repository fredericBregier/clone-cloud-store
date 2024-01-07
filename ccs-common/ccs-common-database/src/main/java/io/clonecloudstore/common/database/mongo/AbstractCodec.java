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

import com.mongodb.MongoClientSettings;
import io.clonecloudstore.common.standard.guid.GuidLike;
import org.bson.BsonReader;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.CollectibleCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import static io.clonecloudstore.common.database.utils.RepositoryBaseInterface.ID;

/**
 * Abstract for Codec implementation. The getEncoderClass method, and some protected ones, are to
 * instantiate.
 *
 * @param <E> type of DTO
 */
public abstract class AbstractCodec<E> implements CollectibleCodec<E> {
  private final Codec<Document> documentCodec;

  protected AbstractCodec() {
    this.documentCodec = MongoClientSettings.getDefaultCodecRegistry().get(Document.class);
  }

  @Override
  public E generateIdIfAbsentFromDocument(final E e) {
    if (!documentHasId(e)) {
      setGuid(e, GuidLike.getGuid());
    }
    return e;
  }

  /**
   * From e, set the given Guid
   */
  protected abstract void setGuid(E e, String guid);

  /**
   * Get the Guid from e
   */
  protected abstract String getGuid(E e);

  @Override
  public boolean documentHasId(final E e) {
    return getGuid(e) != null;
  }

  @Override
  public BsonValue getDocumentId(final E e) {
    return new BsonString(getGuid(e));
  }

  @Override
  public E decode(final BsonReader bsonReader, final DecoderContext decoderContext) {
    final var document = documentCodec.decode(bsonReader, decoderContext);
    final var e = fromDocument(document);
    if (document.getString(ID) != null) {
      setGuid(e, document.getString(ID));
    }
    return e;
  }

  /**
   * Transform the document to E (except GUID)
   */
  protected abstract E fromDocument(Document document);

  @Override
  public void encode(final BsonWriter bsonWriter, final E e, final EncoderContext encoderContext) {
    final var doc = new Document();
    doc.put(ID, getGuid(e));
    toDocument(e, doc);
    documentCodec.encode(bsonWriter, doc, encoderContext);
  }

  /**
   * Transform E to the document (except GUID)
   */
  protected abstract void toDocument(E e, Document document);
}
