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

package io.clonecloudstore.accessor.server.simple.application.buffer;

import java.io.IOException;

import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.accessor.server.commons.AbstractPublicObjectHelper;
import io.clonecloudstore.accessor.server.commons.buffer.CcsBufferService;
import io.clonecloudstore.accessor.server.commons.buffer.FilesystemHandler;
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
@Unremovable
public class CcsBufferAccessorGatewayService extends CcsBufferService {
  @Inject
  FilesystemHandler filesystemHandler;

  @Override
  protected AccessorObject getAccessorObjectFromDb(final String bucket, final String object) {
    try {
      final var storageObject = filesystemHandler.readStorageObject(bucket, object);
      return AbstractPublicObjectHelper.getFromStorageObject(storageObject);
    } catch (final IOException ignore) {
      return null;
    }
  }

  @Override
  protected void updateStatusAccessorObject(final AccessorObject object, final AccessorStatus status) {
    object.setStatus(status);
  }
}
