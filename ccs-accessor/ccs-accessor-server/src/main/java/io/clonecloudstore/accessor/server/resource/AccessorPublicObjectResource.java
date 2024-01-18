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

package io.clonecloudstore.accessor.server.resource;

import java.io.InputStream;

import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.server.application.AccessorObjectService;
import io.clonecloudstore.accessor.server.application.ObjectStreamHandler;
import io.clonecloudstore.accessor.server.commons.AbstractAccessorPublicObjectResource;
import io.quarkus.resteasy.reactive.server.Closer;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;

/**
 * Object API Resource
 */
@Path(AccessorConstants.Api.API_ROOT)
public class AccessorPublicObjectResource extends AbstractAccessorPublicObjectResource<ObjectStreamHandler> {
  public AccessorPublicObjectResource(final AccessorObjectService service) {
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
                                              final String clientId, final String opId) {
    return super.checkObjectOrDirectory(bucketName, pathDirectoryOrObject, clientId, opId);
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

  @Override
  @Blocking
  public Uni<Response> createObject(final HttpServerRequest request, final Closer closer, final String bucketName,
                                    final String objectName, final String contentTypeHeader,
                                    final String contentEncodingHeader, final String clientId, final String opId,
                                    final String xObjectId, final String xObjectSite, final String xObjectBucket,
                                    final String xObjectName, final long xObjectSize, final String xObjectHash,
                                    final String xObjectMetadata, final String xObjectExpires,
                                    final InputStream inputStream) {
    return super.createObject(request, closer, bucketName, objectName, contentTypeHeader, contentEncodingHeader,
        clientId, opId, xObjectId, xObjectSite, xObjectBucket, xObjectName, xObjectSize, xObjectHash, xObjectMetadata,
        xObjectExpires, inputStream);
  }

  @Override
  @Blocking
  public Uni<Response> deleteObject(final String bucketName, final String objectName, final String clientId,
                                    final String opId) {
    return super.deleteObject(bucketName, objectName, clientId, opId);
  }
}
