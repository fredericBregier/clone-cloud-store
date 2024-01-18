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

package io.clonecloudstore.accessor.client.internal.resource.fakeserver;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import io.clonecloudstore.accessor.client.model.AccessorHeaderDtoConverter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.common.quarkus.exception.CcsClientGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.test.server.service.FakeStreamHandlerAbstract;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.RequestScoped;

@RequestScoped
public class FakeStreamHandler extends FakeStreamHandlerAbstract<AccessorObject, AccessorObject> {
  @Override
  protected Map<String, String> getHeaderPushInputStream(final AccessorObject apiBusinessIn, final String finalHash,
                                                         final long size, final AccessorObject apiBusinessOut)
      throws CcsClientGenericException, CcsServerGenericException {
    // Business code should come here (example: headers for object name, object size, ...)
    return new HashMap<>();
  }

  @Override
  protected boolean checkPullAble(final AccessorObject apiBusinessIn, final MultiMap headers)
      throws CcsClientGenericException, CcsServerGenericException {
    return true;
  }

  @Override
  protected Map<String, String> getHeaderPullInputStream(final AccessorObject apiBusinessIn)
      throws CcsClientGenericException, CcsServerGenericException {
    final Map<String, String> map = new HashMap<>();
    AccessorHeaderDtoConverter.objectToMap(
        getBusinessOutForPushAnswer(apiBusinessIn, "hash", FakeObjectInternalService.length), map);
    return map;
  }

  @Override
  protected Map<String, String> getHeaderError(final AccessorObject apiBusinessIn, final int status) {
    // Business code should come here (example: get headers in case of error as Object name, Bucket name...)
    final Map<String, String> map = new HashMap<>();
    AccessorHeaderDtoConverter.objectToMap(apiBusinessIn, map);
    return map;
  }

  @Override
  protected AccessorObject getBusinessOutForPushAnswer(final AccessorObject apiBusinessIn, final String finalHash,
                                                       final long size) {
    AccessorStatus status = apiBusinessIn.getStatus() == null ? AccessorStatus.UNKNOWN : apiBusinessIn.getStatus();
    return new AccessorObject().setBucket(apiBusinessIn.getBucket()).setId(apiBusinessIn.getId())
        .setName(apiBusinessIn.getName()).setSite(apiBusinessIn.getSite()).setExpires(apiBusinessIn.getExpires())
        .setCreation(Instant.now()).setHash(apiBusinessIn.getHash()).setMetadata(apiBusinessIn.getMetadata())
        .setHash(finalHash).setSize(size).setStatus(status);
  }

  @Override
  protected long getLengthFromBusinessIn(final AccessorObject businessIn) {
    return FakeObjectInternalService.length;
  }
}
