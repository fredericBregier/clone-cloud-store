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

package io.clonecloudstore.test.accessor.common;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import io.clonecloudstore.accessor.client.model.AccessorHeaderDtoConverter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericExceptionMapper;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.common.standard.inputstream.MultipleActionsInputStream;
import io.clonecloudstore.common.standard.inputstream.ZstdCompressInputStream;
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
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.X_TYPE;
import static io.clonecloudstore.common.standard.properties.ApiConstants.CHUNKED;
import static io.clonecloudstore.common.standard.properties.ApiConstants.COMPRESSION_ZSTD;
import static io.clonecloudstore.common.standard.properties.ApiConstants.TRANSFER_ENCODING;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_LENGTH;

public class FakeCommonObjectResourceHelper {
  public static int errorCode = 0;
  public static long length = 100;
  public static int nbList = 0;

  public static AccessorObject fromStorageObject(final StorageObject storageObject) {
    return new AccessorObject().setBucket(storageObject.bucket()).setName(storageObject.name())
        .setSite(FakeCommonBucketResourceHelper.site).setId(GuidLike.getGuid())
        .setCreation(storageObject.creationDate()).setSize(storageObject.size()).setHash(storageObject.hash())
        .setStatus(AccessorStatus.READY).setMetadata(storageObject.metadata());
  }

  static StorageObject pseudoList(final AtomicInteger cpt, final String techName, final String namePrefix) {
    return new StorageObject(techName, namePrefix + cpt.incrementAndGet(), "hash", length, Instant.now());
  }

  static void computeList(final Iterator<StorageObject> stream, final UniEmitter<? super Response> em,
                          final HttpServerRequest request, final Closer closer) throws IOException {
    final var inputStream =
        StreamIteratorUtils.getInputStreamFromIterator(stream, source -> fromStorageObject((StorageObject) source),
            AccessorObject.class);
    final var response = Response.ok();
    var map = new HashMap<String, String>();
    map.put(TRANSFER_ENCODING, CHUNKED);
    boolean shallCompress = request.headers().contains(HttpHeaders.ACCEPT_ENCODING, COMPRESSION_ZSTD, true);
    if (shallCompress) {
      map.put(HttpHeaders.CONTENT_ENCODING, COMPRESSION_ZSTD);
    }
    for (final var entry : map.entrySet()) {
      response.header(entry.getKey(), entry.getValue());
    }
    var inputStreamFinal = inputStream;
    if (shallCompress) {
      if (inputStream instanceof MultipleActionsInputStream mai) {
        try {
          mai.decompress();
        } catch (final IOException e) {
          throw new CcsOperationException(e);
        }
      } else {
        try {
          inputStreamFinal = new ZstdCompressInputStream(inputStream);
        } catch (final IOException e) {
          throw new CcsOperationException(e);
        }
      }
      closer.add(inputStreamFinal);
    }
    response.entity(inputStreamFinal);
    em.complete(response.build());
  }

  public static void listObjectsHelper(final UniEmitter<? super Response> em, final String bucketName,
                                       final String xNamePrefix, final String xCreationAfter,
                                       final String xCreationBefore, final String clientId, final boolean isPublic,
                                       final HttpServerRequest request, final Closer closer) {
    var prefixHeader = xNamePrefix;
    if (ParametersChecker.isEmpty(prefixHeader)) {
      prefixHeader = "prefix";
    }
    final var namePrefix = ParametersChecker.getSanitizedName(prefixHeader);
    final var techName = FakeCommonBucketResourceHelper.getBucketTechnicalName(clientId, bucketName, isPublic);
    if (nbList > 0) {
      final var cpt = new AtomicInteger(0);
      final var iterator = Stream.generate(() -> pseudoList(cpt, techName, namePrefix)).limit(nbList).iterator();
      try {
        computeList(iterator, em, request, closer);
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
        computeList(iterator, em, request, closer);
      } catch (final DriverNotFoundException e) {
        em.fail(new CcsNotExistException(e.getMessage()));
      } catch (final DriverException | IOException e) {
        em.fail(new CcsOperationException(e.getMessage()));
      }
    }
  }

  public static Uni<Response> listObjects0Helper(final String bucketName, final String acceptHeader,
                                                 final String acceptEncodingHeader, final String clientId,
                                                 final String opId, final String xNamePrefix, final String xStatuses,
                                                 final String xCreationBefore, final String xCreationAfter,
                                                 final String xExpiresBefore, final String xExpiresAfter,
                                                 final long xSizeLt, final long xSizeGt, final String xMetadataEq,
                                                 final boolean isPublic, final HttpServerRequest request,
                                                 final Closer closer) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode >= 400) {
        throw CcsServerGenericExceptionMapper.getCcsException(errorCode);
      }
      listObjectsHelper(em, bucketName, xNamePrefix, xCreationAfter, xCreationBefore, clientId, isPublic, request,
          closer);
    });
  }

  public static void checkObjectOrDirectoryHelper(final UniEmitter<? super Response> em, final String bucketName,
                                                  final String pathDirectoryOrObject, final boolean fullCheck,
                                                  final String clientId, final boolean isPublic) {
    final var techName = FakeCommonBucketResourceHelper.getBucketTechnicalName(clientId, bucketName, isPublic);
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

  public static Uni<Response> checkObjectOrDirectory0Helper(final String bucketName, final String pathDirectoryOrObject,
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
        checkObjectOrDirectoryHelper(em, bucketName, pathDirectoryOrObject, fullCheck, clientId, isPublic);
      }
    });
  }

  public static void getObjectInfoHelper(final UniEmitter<? super AccessorObject> em, final String bucketName,
                                         final String objectName, final String clientId, final boolean isPublic) {
    final var techName = FakeCommonBucketResourceHelper.getBucketTechnicalName(clientId, bucketName, isPublic);
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

  public static Uni<AccessorObject> getObjectInfo0Helper(final String bucketName, final String objectName,
                                                         final String clientId, final boolean isPublic) {
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
        getObjectInfoHelper(em, bucketName, objectName, clientId, isPublic);
      }
    });
  }

  public static void deleteObjectHelper(final UniEmitter<? super Response> em, final String bucketName,
                                        final String objectName, final String clientId, final boolean isPublic) {
    final var techName = FakeCommonBucketResourceHelper.getBucketTechnicalName(clientId, bucketName, isPublic);
    try (final var fakeDriver = DriverApiRegistry.getDriverApiFactory().getInstance()) {
      fakeDriver.objectDeleteInBucket(techName, ParametersChecker.getSanitizedName(objectName));
      em.complete(Response.noContent().build());
    } catch (final DriverNotFoundException e) {
      em.complete(Response.status(Response.Status.NOT_FOUND).build());
    } catch (final DriverNotAcceptableException e) {
      em.complete(Response.status(Response.Status.NOT_ACCEPTABLE).build());
    } catch (final DriverException e) {
      em.complete(Response.status(Response.Status.INTERNAL_SERVER_ERROR).build());
    }
  }

  public static Uni<Response> deleteObject0Helper(final String bucketName, final String objectName,
                                                  final String clientId, final boolean isPublic) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        if (errorCode >= 400) {
          em.fail(CcsServerGenericExceptionMapper.getCcsException(errorCode));
        } else {
          em.complete(Response.noContent().build());
        }
      } else {
        deleteObjectHelper(em, bucketName, objectName, clientId, isPublic);
      }
    });
  }

  FakeCommonObjectResourceHelper() {
    // Empty
  }

  public static AccessorObject getAccessorObjectForCreate(final HttpServerRequest request, final String bucketName,
                                                          final String objectName, final String clientId,
                                                          final long xObjectSize, final boolean isPublic) {
    final var techName = FakeCommonBucketResourceHelper.getBucketTechnicalName(clientId, bucketName, isPublic);
    var storeLength = xObjectSize;
    if (storeLength <= 0) {
      var headerLength = request.getHeader(CONTENT_LENGTH);
      if (ParametersChecker.isNotEmpty(headerLength)) {
        storeLength = Long.parseLong(headerLength);
      }
    }
    final var accessorObject = new AccessorObject();
    AccessorHeaderDtoConverter.objectFromMap(accessorObject, request.headers());
    accessorObject.setName(ParametersChecker.getSanitizedName(objectName)).setSite(FakeCommonBucketResourceHelper.site)
        .setBucket(techName).setSize(storeLength);
    return accessorObject;
  }

  public static AccessorObject getAccessorObjectForGetObject(final String bucketName, final String objectName,
                                                             final String clientId, final boolean isPublic) {
    final var techName = FakeCommonBucketResourceHelper.getBucketTechnicalName(clientId, bucketName, isPublic);
    return new AccessorObject().setName(ParametersChecker.getSanitizedName(objectName))
        .setSite(FakeCommonBucketResourceHelper.site).setBucket(techName);
  }
}
