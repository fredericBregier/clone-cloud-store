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

package io.clonecloudstore.common.database.mongo.impl.codec;

import io.clonecloudstore.common.database.mongo.AbstractCodec;
import org.bson.Document;

import static io.clonecloudstore.common.database.model.dao.DaoExampleRepository.FIELD1;
import static io.clonecloudstore.common.database.model.dao.DaoExampleRepository.FIELD2;
import static io.clonecloudstore.common.database.model.dao.DaoExampleRepository.TIME_FIELD;

public class MgDaoExampleCodec extends AbstractCodec<MgDaoExample> {
  public MgDaoExampleCodec() {
    super();
  }

  @Override
  protected void setGuid(final MgDaoExample mgDbDtoExample, final String guid) {
    mgDbDtoExample.setGuid(guid);
  }

  @Override
  protected String getGuid(final MgDaoExample mgDbDtoExample) {
    return mgDbDtoExample.getGuid();
  }

  @Override
  protected MgDaoExample fromDocument(final Document document) {
    final var mgDbDtoExample = new MgDaoExample();
    mgDbDtoExample.setField1(document.getString(FIELD1));
    mgDbDtoExample.setField2(document.getString(FIELD2));
    mgDbDtoExample.setTimeField(document.getDate(TIME_FIELD).toInstant());
    return mgDbDtoExample;
  }

  @Override
  protected void toDocument(final MgDaoExample mgDbDtoExample, final Document document) {
    document.put(FIELD1, mgDbDtoExample.getField1());
    document.put(FIELD2, mgDbDtoExample.getField2());
    document.put(TIME_FIELD, mgDbDtoExample.getTimeField());
  }

  @Override
  public Class<MgDaoExample> getEncoderClass() {
    return MgDaoExample.class;
  }
}
