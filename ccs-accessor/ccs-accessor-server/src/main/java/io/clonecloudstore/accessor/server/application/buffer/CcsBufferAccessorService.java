/*
 * Copyright (c) 2024. Clone Cloud Store (CCS), Contributors and Frederic Bregier
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

package io.clonecloudstore.accessor.server.application.buffer;

import java.time.Instant;

import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.commons.buffer.CcsBufferService;
import io.clonecloudstore.accessor.server.database.model.DaoAccessorObjectRepository;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;

@ApplicationScoped
@Unremovable
public class CcsBufferAccessorService extends CcsBufferService {
  private final DaoAccessorObjectRepository objectRepository;

  public CcsBufferAccessorService(final Instance<DaoAccessorObjectRepository> objectRepositoryInstance) {
    this.objectRepository = objectRepositoryInstance.get();
  }


  @Override
  protected AccessorObject getAccessorObjectFromDb(final String bucket, final String object) {
    try {
      final var dao = objectRepository.getObject(bucket, object);
      if (dao != null) {
        return dao.getDto();
      }
      return null;
    } catch (final CcsDbException e) {
      return null;
    }
  }

  @Override
  protected void updateStatusAccessorObject(final AccessorObject object, final AccessorStatus status) {
    try {
      objectRepository.updateObjectStatus(object.getBucket(), object.getName(), status, Instant.now());
    } catch (CcsDbException ignore) {
      // ignore
    }
  }
}
