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

package io.clonecloudstore.administration.database.mongodb;

import io.clonecloudstore.administration.database.model.DaoOwnershipRepository;
import io.clonecloudstore.administration.database.mongodb.conf.Constants;
import io.clonecloudstore.administration.model.ClientBucketAccess;
import io.clonecloudstore.administration.model.ClientOwnership;
import io.quarkus.test.junit.QuarkusTest;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
class MgDaoOwnershipTest {
  private static final Logger logger = Logger.getLogger(MgDaoOwnershipTest.class);


  @Test
  void checkDaoInitialisationFromDto() {
    logger.debugf("\n\nTesting ownership dao bean");
    final var dto =
        new ClientBucketAccess(Constants.OWNERSHIP_CLIENT_ID, Constants.OWNERSHIP_BUCKET, ClientOwnership.READ);
    final var dao = new MgDaoOwnership().setId(
            DaoOwnershipRepository.getInternalId(Constants.OWNERSHIP_CLIENT_ID, Constants.OWNERSHIP_BUCKET))
        .setClientId(Constants.OWNERSHIP_CLIENT_ID).setBucket(Constants.OWNERSHIP_BUCKET)
        .setOwnership(ClientOwnership.READ);

    final var daoFromDto = new MgDaoOwnership(dto);

    assertEquals(daoFromDto, dao);
    assertEquals(daoFromDto.hashCode(), dao.hashCode());
  }
}
