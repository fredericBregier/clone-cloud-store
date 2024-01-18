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

import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.server.commons.AbstractPrivateObjectHelper;
import io.clonecloudstore.accessor.server.commons.AccessorObjectServiceInterface;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.quarkus.resteasy.reactive.server.Closer;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.UniEmitter;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.Dependent;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;

@Dependent
public abstract class FakeObjectPrivateAbstract<H extends FakeStreamHandlerAbstract>
    extends AbstractPrivateObjectHelper<H> {
  HttpHeaders httpHeaders;

  protected FakeObjectPrivateAbstract(final HttpHeaders httpHeaders) {
    super((AccessorObjectServiceInterface) null);
    this.httpHeaders = httpHeaders;
  }

  protected boolean isPublic() {
    return false;
  }

  protected void listObjects(final UniEmitter<? super Response> em, final String bucketName, final String xNamePrefix,
                             final String xCreationAfter, final String xCreationBefore, final String clientId,
                             final HttpServerRequest request, final Closer closer) {
    FakeCommonObjectResourceHelper.listObjectsHelper(em, bucketName, xNamePrefix, xCreationAfter, xCreationBefore,
        clientId, request, closer);
  }

  protected void checkObjectOrDirectory(final UniEmitter<? super Response> em, final String bucketName,
                                        final String pathDirectoryOrObject, final String clientId) {
    FakeCommonObjectResourceHelper.checkObjectOrDirectoryHelper(em, bucketName, pathDirectoryOrObject, clientId);
  }

  protected Uni<Response> checkObjectOrDirectory0(final String bucketName, final String pathDirectoryOrObject,
                                                  final boolean fullCheck, final String clientId) {
    return FakeCommonObjectResourceHelper.checkObjectOrDirectory0Helper(bucketName, pathDirectoryOrObject, clientId);
  }

  protected void getObjectInfo(final UniEmitter<? super AccessorObject> em, final String bucketName,
                               final String objectName, final String clientId) {
    FakeCommonObjectResourceHelper.getObjectInfoHelper(em, bucketName, objectName, clientId);
  }

  protected Uni<Response> getObject0(final String bucketName, final String objectName, final String acceptHeader,
                                     final String acceptEncodingHeader, final String clientId, final String opId,
                                     final HttpServerRequest request, final Closer closer) {
    final var accessorObject = new AccessorObject().setName(ParametersChecker.getSanitizedObjectName(objectName))
        .setSite(FakeCommonBucketResourceHelper.site).setBucket(bucketName);
    return readObject(request, closer, accessorObject, false);
  }
}
