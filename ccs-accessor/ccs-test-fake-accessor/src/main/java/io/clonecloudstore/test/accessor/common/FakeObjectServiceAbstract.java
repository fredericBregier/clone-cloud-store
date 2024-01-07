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

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import io.clonecloudstore.accessor.client.model.AccessorHeaderDtoConverter;
import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericExceptionMapper;
import io.clonecloudstore.common.quarkus.server.service.StreamServiceAbstract;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.common.standard.stream.StreamIteratorUtils;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.DriverApiRegistry;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotAcceptableException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.quarkus.resteasy.reactive.server.Closer;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.UniEmitter;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.enums.ParameterIn;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.X_CLIENT_ID;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.X_TYPE;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderFilterObject.FILTER_CREATION_AFTER;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderFilterObject.FILTER_CREATION_BEFORE;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderFilterObject.FILTER_EXPIRES_AFTER;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderFilterObject.FILTER_EXPIRES_BEFORE;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderFilterObject.FILTER_METADATA_EQ;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderFilterObject.FILTER_NAME_PREFIX;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderFilterObject.FILTER_SIZE_GT;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderFilterObject.FILTER_SIZE_LT;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderFilterObject.FILTER_STATUSES;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderObject.X_OBJECT_BUCKET;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderObject.X_OBJECT_EXPIRES;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderObject.X_OBJECT_HASH;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderObject.X_OBJECT_ID;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderObject.X_OBJECT_METADATA;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderObject.X_OBJECT_NAME;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderObject.X_OBJECT_SITE;
import static io.clonecloudstore.accessor.config.AccessorConstants.HeaderObject.X_OBJECT_SIZE;
import static io.clonecloudstore.common.quarkus.client.SimpleClientAbstract.X_OP_ID;
import static jakarta.ws.rs.core.HttpHeaders.ACCEPT;
import static jakarta.ws.rs.core.HttpHeaders.ACCEPT_ENCODING;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_ENCODING;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_LENGTH;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_TYPE;

@Dependent
public abstract class FakeObjectServiceAbstract<H extends FakeNativeStreamHandlerAbstract>
    extends StreamServiceAbstract<AccessorObject, AccessorObject, H> {
  public static int errorCode = 0;
  public static long length = 100;
  public static int nbList = 0;
  @Inject
  HttpHeaders httpHeaders;

  protected abstract boolean isPublic();

  public static AccessorObject fromStorageObject(final StorageObject storageObject) {
    return new AccessorObject().setBucket(storageObject.bucket()).setName(storageObject.name())
        .setSite(FakeBucketServiceAbstract.site).setId(GuidLike.getGuid()).setCreation(storageObject.creationDate())
        .setSize(storageObject.size()).setHash(storageObject.hash()).setStatus(AccessorStatus.READY)
        .setMetadata(storageObject.metadata());
  }

  private static StorageObject pseudoList(final AtomicInteger cpt, final String techName, final String namePrefix) {
    return new StorageObject(techName, namePrefix + cpt.incrementAndGet(), "hash", length, Instant.now());
  }

  private void computeList(final Iterator<StorageObject> stream, final UniEmitter<? super InputStream> em)
      throws IOException {
    final var inputStream =
        StreamIteratorUtils.getInputStreamFromIterator(stream, source -> fromStorageObject((StorageObject) source),
            AccessorObject.class);
    em.complete(inputStream);
  }

  protected void listObjects(final UniEmitter<? super InputStream> em, final String bucketName,
                             final String xNamePrefix, final String xCreationAfter, final String xCreationBefore,
                             final String clientId, final boolean isPublic, final HttpServerRequest request,
                             final Closer closer) {
    var prefixHeader = xNamePrefix;
    if (ParametersChecker.isEmpty(prefixHeader)) {
      prefixHeader = "prefix";
    }
    final var namePrefix = ParametersChecker.getSanitizedName(prefixHeader);
    final var techName = FakeBucketServiceAbstract.getBucketTechnicalName(clientId, bucketName, isPublic);
    if (nbList > 0) {
      final var cpt = new AtomicInteger(0);
      final var iterator = Stream.generate(() -> pseudoList(cpt, techName, namePrefix)).limit(nbList).iterator();
      try {
        computeList(iterator, em);
      } catch (final IOException e) {
        em.fail(new CcsOperationException(e.getMessage()));
      }
    } else {
      try (final var fakeDriver = DriverApiRegistry.getDriverApiFactory().getInstance()) {
        Instant from = null;
        Instant to = null;
        if (ParametersChecker.isNotEmpty(xCreationAfter)) {
          from = Instant.parse(xCreationAfter);
        }
        if (ParametersChecker.isNotEmpty(xCreationBefore)) {
          to = Instant.parse(xCreationBefore);
        }
        final var iterator = fakeDriver.objectsIteratorInBucket(techName, namePrefix, from, to);
        computeList(iterator, em);
      } catch (final DriverNotFoundException e) {
        em.fail(new CcsNotExistException(e.getMessage()));
      } catch (final DriverException | IOException e) {
        em.fail(new CcsOperationException(e.getMessage()));
      }
    }
  }

  protected Uni<InputStream> listObjects0(@PathParam("bucketName") final String bucketName,
                                          @HeaderParam(ACCEPT) final String acceptHeader,
                                          @DefaultValue("") @HeaderParam(ACCEPT_ENCODING) final String acceptEncodingHeader,
                                          @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client " +
                                              "ID", in = ParameterIn.HEADER, schema = @Schema(type =
                                              SchemaType.STRING), required = true) @HeaderParam(X_CLIENT_ID) final String clientId,
                                          @Parameter(name = X_OP_ID, description = "Operation ID", in =
                                              ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING),
                                              required = false) @DefaultValue("") @HeaderParam(X_OP_ID) final String opId,
                                          @DefaultValue("") @HeaderParam(FILTER_NAME_PREFIX) final String xNamePrefix,
                                          @DefaultValue("") @HeaderParam(FILTER_STATUSES) final String xStatuses,
                                          @DefaultValue("") @HeaderParam(FILTER_CREATION_BEFORE) final String xCreationBefore,
                                          @DefaultValue("") @HeaderParam(FILTER_CREATION_AFTER) final String xCreationAfter,
                                          @DefaultValue("") @HeaderParam(FILTER_EXPIRES_BEFORE) final String xExpiresBefore,
                                          @DefaultValue("") @HeaderParam(FILTER_EXPIRES_AFTER) final String xExpiresAfter,
                                          @DefaultValue("0") @HeaderParam(FILTER_SIZE_LT) final long xSizeLt,
                                          @DefaultValue("0") @HeaderParam(FILTER_SIZE_GT) final long xSizeGt,
                                          @DefaultValue("") @HeaderParam(FILTER_METADATA_EQ) final String xMetadataEq,
                                          final boolean isPublic, final HttpServerRequest request,
                                          @Context final Closer closer) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode >= 400) {
        throw CcsServerGenericExceptionMapper.getCcsException(errorCode);
      }
      listObjects(em, bucketName, xNamePrefix, xCreationAfter, xCreationBefore, clientId, isPublic, request, closer);
    });
  }

  protected void checkObjectOrDirectory(final UniEmitter<? super Response> em, final String bucketName,
                                        final String pathDirectoryOrObject, final boolean fullCheck,
                                        final String clientId, final boolean isPublic) {
    final var techName = FakeBucketServiceAbstract.getBucketTechnicalName(clientId, bucketName, isPublic);
    try (final var fakeDriver = DriverApiRegistry.getDriverApiFactory().getInstance()) {
      final var storageType = fakeDriver.directoryOrObjectExistsInBucket(techName,
          ParametersChecker.getSanitizedName(pathDirectoryOrObject));
      if (storageType.equals(StorageType.NONE)) {
        em.complete(Response.status(Response.Status.NOT_FOUND).header(X_TYPE, StorageType.NONE).build());
      } else {
        em.complete(Response.ok().header(X_TYPE, storageType).build());
      }
    } catch (final DriverException e) {
      em.complete(Response.status(Response.Status.INTERNAL_SERVER_ERROR).build());
    }
  }

  protected Uni<Response> checkObjectOrDirectory0(final String bucketName, final String pathDirectoryOrObject,
                                                  final boolean fullCheck, final String clientId,
                                                  final boolean isPublic) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        if (errorCode >= 400 && errorCode != 404) {
          em.fail(CcsServerGenericExceptionMapper.getCcsException(errorCode));
        } else if (errorCode == 404) {
          em.complete((Response.status(Response.Status.NOT_FOUND).header(X_TYPE, StorageType.NONE).build()));
        } else {
          em.complete((Response.status(Response.Status.NO_CONTENT).header(X_TYPE, StorageType.OBJECT).build()));
        }
      } else {
        checkObjectOrDirectory(em, bucketName, pathDirectoryOrObject, fullCheck, clientId, isPublic);
      }
    });
  }

  protected void getObjectInfo(final UniEmitter<? super AccessorObject> em, final String bucketName,
                               final String objectName, final String clientId, final boolean isPublic) {
    final var techName = FakeBucketServiceAbstract.getBucketTechnicalName(clientId, bucketName, isPublic);
    try (final var fakeDriver = DriverApiRegistry.getDriverApiFactory().getInstance()) {
      final var storageObject =
          fakeDriver.objectGetMetadataInBucket(techName, ParametersChecker.getSanitizedName(objectName));
      em.complete(fromStorageObject(storageObject));
    } catch (final DriverNotFoundException e) {
      em.fail(new CcsNotExistException(e.getMessage(), e));
    } catch (final DriverException e) {
      em.fail(new CcsOperationException(e));
    }
  }

  protected Uni<AccessorObject> getObjectInfo0(final String bucketName, final String objectName, final String clientId,
                                               final boolean isPublic) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        if (errorCode >= 400) {
          em.fail(CcsServerGenericExceptionMapper.getCcsException(errorCode));
        } else {
          final var object = new AccessorObject().setId(GuidLike.getGuid()).setBucket(bucketName).setName(objectName)
              .setCreation(Instant.now().minusSeconds(100)).setSize(100).setStatus(AccessorStatus.READY);
          em.complete(object);
        }
      } else {
        getObjectInfo(em, bucketName, objectName, clientId, isPublic);
      }
    });
  }

  /**
   * Could be overridden to take into account remote delete
   */
  protected abstract void remoteDeleteObject(AccessorObject accessorObject, String clientId);

  protected void deleteObject(final UniEmitter<? super Response> em, final String bucketName, final String objectName,
                              final String clientId, final boolean isPublic) {
    final var techName = FakeBucketServiceAbstract.getBucketTechnicalName(clientId, bucketName, isPublic);
    try (final var fakeDriver = DriverApiRegistry.getDriverApiFactory().getInstance()) {
      fakeDriver.objectDeleteInBucket(techName, ParametersChecker.getSanitizedName(objectName));
      final var accessorObject =
          new AccessorObject().setBucket(techName).setName(objectName).setSite(FakeBucketServiceAbstract.site)
              .setId(GuidLike.getGuid()).setStatus(AccessorStatus.DELETING);
      remoteDeleteObject(accessorObject, clientId);
      em.complete(Response.noContent().build());
    } catch (final DriverNotFoundException e) {
      em.complete(Response.status(Response.Status.NOT_FOUND).build());
    } catch (final DriverNotAcceptableException e) {
      em.complete(Response.status(Response.Status.NOT_ACCEPTABLE).build());
    } catch (final DriverException e) {
      em.complete(Response.status(Response.Status.INTERNAL_SERVER_ERROR).build());
    }
  }

  protected Uni<Response> deleteObject0(final String bucketName, final String objectName, final String clientId,
                                        final boolean isPublic) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        if (errorCode >= 400) {
          em.fail(CcsServerGenericExceptionMapper.getCcsException(errorCode));
        } else {
          em.complete(Response.noContent().build());
        }
      } else {
        deleteObject(em, bucketName, objectName, clientId, isPublic);
      }
    });
  }

  protected Uni<Response> createObject0(final HttpServerRequest request, @Context final Closer closer,
                                        @PathParam("bucketName") final String bucketName,
                                        @PathParam("objectName") final String objectName,
                                        @DefaultValue(MediaType.APPLICATION_OCTET_STREAM) @HeaderParam(CONTENT_TYPE) final String contentTypeHeader,
                                        @DefaultValue("") @HeaderParam(CONTENT_ENCODING) final String contentEncodingHeader,
                                        @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client " +
                                            "ID", in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING)
                                            , required = true) @HeaderParam(X_CLIENT_ID) final String clientId,
                                        @Parameter(name = X_OP_ID, description = "Operation ID", in =
                                            ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING), required
                                            = false) @DefaultValue("") @HeaderParam(X_OP_ID) final String opId,
                                        @DefaultValue("") @HeaderParam(X_OBJECT_ID) final String xObjectId,
                                        @DefaultValue("") @HeaderParam(X_OBJECT_SITE) final String xObjectSite,
                                        @DefaultValue("") @HeaderParam(X_OBJECT_BUCKET) final String xObjectBucket,
                                        @DefaultValue("") @HeaderParam(X_OBJECT_NAME) final String xObjectName,
                                        @DefaultValue("0") @HeaderParam(X_OBJECT_SIZE) final long xObjectSize,
                                        @DefaultValue("") @HeaderParam(X_OBJECT_HASH) final String xObjectHash,
                                        @DefaultValue("") @HeaderParam(X_OBJECT_METADATA) final String xObjectMetadata,
                                        @DefaultValue("") @HeaderParam(X_OBJECT_EXPIRES) final String xObjectExpires,
                                        final boolean isPublic, final InputStream inputStream) {
    final var techName = FakeBucketServiceAbstract.getBucketTechnicalName(clientId, bucketName, isPublic);
    var storeLength = xObjectSize;
    if (storeLength <= 0) {
      var headerLength = httpHeaders.getHeaderString(CONTENT_LENGTH);
      if (ParametersChecker.isNotEmpty(headerLength)) {
        storeLength = Long.parseLong(headerLength);
      }
    }
    final var accessorObject = new AccessorObject();
    AccessorHeaderDtoConverter.objectFromMap(accessorObject, request.headers());
    accessorObject.setName(ParametersChecker.getSanitizedName(objectName)).setSite(FakeBucketServiceAbstract.site)
        .setBucket(techName).setSize(storeLength);
    // TODO choose compression model
    return createObject(request, closer, accessorObject, accessorObject.getSize(), accessorObject.getHash(), false,
        inputStream);
  }

  protected Uni<Response> getObject0(@PathParam("bucketName") final String bucketName,
                                     @PathParam("objectName") final String objectName,
                                     @HeaderParam(ACCEPT) final String acceptHeader,
                                     @DefaultValue("") @HeaderParam(ACCEPT_ENCODING) final String acceptEncodingHeader,
                                     @Parameter(name = AccessorConstants.Api.X_CLIENT_ID, description = "Client ID",
                                         in = ParameterIn.HEADER, schema = @Schema(type = SchemaType.STRING),
                                         required = true) @HeaderParam(X_CLIENT_ID) final String clientId,
                                     @Parameter(name = X_OP_ID, description = "Operation ID", in = ParameterIn.HEADER
                                         , schema = @Schema(type = SchemaType.STRING), required = false) @DefaultValue("") @HeaderParam(X_OP_ID) final String opId,
                                     final boolean isPublic, final HttpServerRequest request,
                                     @Context final Closer closer) {
    final var techName = FakeBucketServiceAbstract.getBucketTechnicalName(clientId, bucketName, isPublic);
    final var accessorObject = new AccessorObject().setName(ParametersChecker.getSanitizedName(objectName))
        .setSite(FakeBucketServiceAbstract.site).setBucket(techName);
    return readObject(request, closer, accessorObject, false);
  }
}
