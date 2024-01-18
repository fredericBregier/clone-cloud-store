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

package io.clonecloudstore.test.accessor.server.resource;

import java.io.InputStream;

import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.test.accessor.common.FakeAccessorObjectPublicAbstract;
import io.clonecloudstore.test.accessor.common.FakeStreamHandlerAbstract;
import io.quarkus.resteasy.reactive.server.Closer;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.Dependent;
import jakarta.ws.rs.core.Response;

@Dependent
public abstract class FakeObjectPublicResourceAbstract<H extends FakeStreamHandlerAbstract>
    extends FakeAccessorObjectPublicAbstract<H> {
  @Override
  public Uni<Response> listObjects(final String bucketName, final String acceptHeader,
                                   final String acceptEncodingHeader, final String clientId, final String opId,
                                   final String xNamePrefix, final String xStatuses, final String xCreationBefore,
                                   final String xCreationAfter, final String xExpiresBefore, final String xExpiresAfter,
                                   final long xSizeLt, final long xSizeGt, final String xMetadataEq,
                                   final HttpServerRequest request, final Closer closer) {
    return listObjects0(bucketName, clientId, xNamePrefix, xStatuses, xCreationBefore, xCreationAfter, xExpiresBefore,
        xExpiresAfter, xSizeLt, xSizeGt, xMetadataEq, request, closer);
  }

  @Override
  public Uni<Response> checkObjectOrDirectory(final String bucketName, final String pathDirectoryOrObject,
                                              final String clientId, final String opId) {
    final var decodedName = ParametersChecker.getSanitizedObjectName(pathDirectoryOrObject);
    return checkObjectOrDirectory0(bucketName, decodedName, clientId);
  }

  @Override
  public Uni<AccessorObject> getObjectInfo(final String bucketName, final String objectName, final String clientId,
                                           final String opId) {
    final var decodedName = ParametersChecker.getSanitizedObjectName(objectName);
    return getObjectInfo0(bucketName, decodedName, clientId);
  }

  @Override
  public Uni<Response> deleteObject(final String bucketName, final String objectName, final String clientId,
                                    final String opId) {
    final var decodedName = ParametersChecker.getSanitizedObjectName(objectName);
    return deleteObject0(bucketName, decodedName, clientId);
  }

  @Override
  public Uni<Response> createObject(final HttpServerRequest request, final Closer closer, final String bucketName,
                                    final String objectName, final String contentTypeHeader,
                                    final String contentEncodingHeader, final String clientId, final String opId,
                                    final String xObjectId, final String xObjectSite, final String xObjectBucket,
                                    final String xObjectName, final long xObjectSize, final String xObjectHash,
                                    final String xObjectMetadata, final String xObjectExpires,
                                    final InputStream inputStream) {
    final var decodedName = ParametersChecker.getSanitizedObjectName(objectName);
    return createObject0(request, closer, bucketName, decodedName, contentTypeHeader, contentEncodingHeader, clientId,
        opId, xObjectId, xObjectSite, xObjectBucket, xObjectName, xObjectSize, xObjectHash, xObjectMetadata,
        xObjectExpires, inputStream);
  }

  @Override
  public Uni<Response> getObject(final String bucketName, final String objectName, final String acceptHeader,
                                 final String acceptEncodingHeader, final String clientId, final String opId,
                                 final HttpServerRequest request, final Closer closer) {
    final var decodedName = ParametersChecker.getSanitizedObjectName(objectName);
    return getObject0(bucketName, decodedName, acceptHeader, acceptEncodingHeader, clientId, opId, request, closer);
  }
}
