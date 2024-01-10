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

package io.clonecloudstore.accessor.server.resource.internal;

import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.server.application.AccessorObjectService;
import io.clonecloudstore.accessor.server.application.ObjectNativeStreamHandler;
import io.clonecloudstore.accessor.server.commons.AbstractAccessorPrivateObjectResource;
import io.quarkus.resteasy.reactive.server.Closer;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.INTERNAL_ROOT;

/**
 * Object API Resource
 */
@Path(INTERNAL_ROOT)
public class AccessorObjectInternalResource extends AbstractAccessorPrivateObjectResource<ObjectNativeStreamHandler> {
  public AccessorObjectInternalResource(final AccessorObjectService service) {
    super(service);
  }

  @Override
  @Blocking
  public Uni<Response> listObjects(final String bucketName, final String acceptHeader,
                                   final String acceptEncodingHeader, final String clientId, final String opId,
                                   final String xNamePrefix, final String xStatuses, final String xCreationBefore,
                                   final String xCreationAfter, final String xExpiresBefore, final String xExpiresAfter,
                                   final long xSizeLt, final long xSizeGt, final String xMetadataEq,
                                   final HttpServerRequest request, final Closer closer) {
    return super.listObjects(bucketName, acceptHeader, acceptEncodingHeader, clientId, opId, xNamePrefix, xStatuses,
        xCreationBefore, xCreationAfter, xExpiresBefore, xExpiresAfter, xSizeLt, xSizeGt, xMetadataEq, request, closer);
  }

  @Override
  @Blocking
  public Uni<Response> checkObjectOrDirectory(final String bucketName, final String pathDirectoryOrObject,
                                              final boolean fullCheck, final String clientId, final String opId) {
    return super.checkObjectOrDirectory(bucketName, pathDirectoryOrObject, fullCheck, clientId, opId);
  }

  @Override
  @Blocking
  public Uni<AccessorObject> getObjectInfo(final String bucketName, final String objectName, final String clientId,
                                           final String opId) {
    return super.getObjectInfo(bucketName, objectName, clientId, opId);
  }

  @Override
  @Blocking
  public Uni<Response> getObject(final String bucketName, final String objectName, final String acceptHeader,
                                 final String acceptEncodingHeader, final String clientId, final String opId,
                                 final HttpServerRequest request, final Closer closer) {
    return super.getObject(bucketName, objectName, acceptHeader, acceptEncodingHeader, clientId, opId, request, closer);
  }
}
