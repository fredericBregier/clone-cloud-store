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

package io.clonecloudstore.test.accessor.common;

import java.io.InputStream;

import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.server.commons.AbstractAccessorPublicObjectResource;
import io.clonecloudstore.accessor.server.commons.AccessorObjectServiceInterface;
import io.quarkus.resteasy.reactive.server.Closer;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.UniEmitter;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.Dependent;
import jakarta.ws.rs.core.Response;

@Dependent
public abstract class FakeAccessorObjectPublicAbstract<H extends FakeStreamHandlerAbstract>
    extends AbstractAccessorPublicObjectResource<H> {
  protected FakeAccessorObjectPublicAbstract() {
    super((AccessorObjectServiceInterface) null);
  }

  protected boolean isPublic() {
    return true;
  }

  protected void listObjects(final UniEmitter<? super Response> em, final String bucketName, final String xNamePrefix,
                             final String xCreationAfter, final String xCreationBefore, final String clientId,
                             final HttpServerRequest request, final Closer closer) {
    FakeCommonObjectResourceHelper.listObjectsHelper(em, bucketName, xNamePrefix, xCreationAfter, xCreationBefore,
        clientId, request, closer);
  }

  protected Uni<Response> listObjects0(final String bucketName, final String clientId, final String xNamePrefix,
                                       final String xStatuses, final String xCreationBefore,
                                       final String xCreationAfter, final String xExpiresBefore,
                                       final String xExpiresAfter, final long xSizeLt, final long xSizeGt,
                                       final String xMetadataEq, final HttpServerRequest request, final Closer closer) {
    return FakeCommonObjectResourceHelper.listObjects0Helper(bucketName, clientId, xNamePrefix, xStatuses,
        xCreationBefore, xCreationAfter, xExpiresBefore, xExpiresAfter, xSizeLt, xSizeGt, xMetadataEq, request, closer);
  }

  protected void checkObjectOrDirectory(final UniEmitter<? super Response> em, final String bucketName,
                                        final String pathDirectoryOrObject, final String clientId) {
    FakeCommonObjectResourceHelper.checkObjectOrDirectoryHelper(em, bucketName, pathDirectoryOrObject, clientId);
  }

  protected Uni<Response> checkObjectOrDirectory0(final String bucketName, final String pathDirectoryOrObject,
                                                  final String clientId) {
    return FakeCommonObjectResourceHelper.checkObjectOrDirectory0Helper(bucketName, pathDirectoryOrObject, clientId);
  }

  protected void getObjectInfo(final UniEmitter<? super AccessorObject> em, final String bucketName,
                               final String objectName, final String clientId) {
    FakeCommonObjectResourceHelper.getObjectInfoHelper(em, bucketName, objectName, clientId);
  }

  protected Uni<AccessorObject> getObjectInfo0(final String bucketName, final String objectName,
                                               final String clientId) {
    return FakeCommonObjectResourceHelper.getObjectInfo0Helper(bucketName, objectName, clientId);
  }

  protected Uni<Response> deleteObject0(final String bucketName, final String objectName, final String clientId) {
    return FakeCommonObjectResourceHelper.deleteObject0Helper(bucketName, objectName, clientId);
  }

  protected Uni<Response> createObject0(final HttpServerRequest request, final Closer closer, final String bucketName,
                                        final String objectName, final String contentTypeHeader,
                                        final String contentEncodingHeader, final String clientId, final String opId,
                                        final String xObjectId, final String xObjectSite, final String xObjectBucket,
                                        final String xObjectName, final long xObjectSize, final String xObjectHash,
                                        final String xObjectMetadata, final String xObjectExpires,
                                        final InputStream inputStream) {
    final var accessorObject =
        FakeCommonObjectResourceHelper.getAccessorObjectForCreate(request, bucketName, objectName, clientId,
            xObjectSize);
    return createObject(request, closer, accessorObject, accessorObject.getSize(), accessorObject.getHash(),
        inputStream);
  }

  protected Uni<Response> getObject0(final String bucketName, final String objectName, final String acceptHeader,
                                     final String acceptEncodingHeader, final String clientId, final String opId,
                                     final HttpServerRequest request, final Closer closer) {
    final var accessorObject =
        FakeCommonObjectResourceHelper.getAccessorObjectForGetObject(bucketName, objectName, clientId);
    return readObject(request, closer, accessorObject, true);
  }

}
