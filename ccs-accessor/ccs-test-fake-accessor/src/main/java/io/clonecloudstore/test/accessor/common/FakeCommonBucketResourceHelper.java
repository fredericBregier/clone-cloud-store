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

import java.time.Instant;
import java.util.Collection;

import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.model.AccessorStatus;
import io.clonecloudstore.common.quarkus.exception.CcsAlreadyExistException;
import io.clonecloudstore.common.quarkus.exception.CcsNotAcceptableException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.exception.CcsServerExceptionMapper;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;
import io.clonecloudstore.driver.api.DriverApiRegistry;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverAlreadyExistException;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotAcceptableException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.UniEmitter;
import jakarta.ws.rs.core.Response;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.X_TYPE;

public class FakeCommonBucketResourceHelper {
  public static int errorCode = 0;
  public static String site = "site";

  protected static AccessorBucket fromStorageBucket(final StorageBucket storageBucket, final String clientId) {
    return new AccessorBucket().setSite(site).setId(storageBucket.bucket()).setCreation(storageBucket.creationDate())
        .setStatus(AccessorStatus.READY).setClientId(clientId);
  }

  protected static AccessorBucket fromQueryParameter(final String bucketName, final String clientId,
                                                     final AccessorStatus status) {
    return new AccessorBucket().setSite(site).setId(bucketName).setCreation(Instant.now()).setStatus(status)
        .setClientId(clientId);
  }

  public static void getBucketsHelper(final UniEmitter<? super Collection<AccessorBucket>> em, final String clientId) {
    try (final var fakeDriver = DriverApiRegistry.getDriverApiFactory().getInstance()) {
      final var list = fakeDriver.bucketsStream().filter(storageBucket -> storageBucket.clientId().equals(clientId))
          .map(storageBucket -> fromStorageBucket(storageBucket, clientId)).toList();
      em.complete(list);
    } catch (final DriverException e) {
      em.fail(e);
    }
  }

  public static Uni<Collection<AccessorBucket>> getBuckets0Helper(final String clientId) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        em.fail(CcsServerExceptionMapper.getCcsException(errorCode));
      } else {
        getBucketsHelper(em, clientId);
      }
    });
  }

  public static void checkBucketHelper(final UniEmitter<? super Response> em, final String bucketName) {
    try (final var fakeDriver = DriverApiRegistry.getDriverApiFactory().getInstance()) {
      if (fakeDriver.bucketExists(bucketName)) {
        em.complete(Response.ok().header(X_TYPE, StorageType.BUCKET).build());
      } else {
        em.complete((Response.status(Response.Status.NOT_FOUND).header(X_TYPE, StorageType.NONE).build()));
      }
    } catch (final DriverException e) {
      em.complete(Response.status(Response.Status.INTERNAL_SERVER_ERROR).build());
    }
  }

  public static Uni<Response> checkBucket0Helper(final String bucketName) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        if (errorCode >= 400 && errorCode != 404) {
          em.fail(CcsServerExceptionMapper.getCcsException(errorCode));
        } else if (errorCode != 204) {
          em.complete((Response.status(Response.Status.NOT_FOUND).header(X_TYPE, StorageType.NONE).build()));
        } else {
          em.complete((Response.status(Response.Status.NO_CONTENT).header(X_TYPE, StorageType.BUCKET).build()));
        }
      } else {
        checkBucketHelper(em, bucketName);
      }
    });
  }

  public static void getBucketHelper(final UniEmitter<? super AccessorBucket> em, final String bucketName) {
    try (final var fakeDriver = DriverApiRegistry.getDriverApiFactory().getInstance()) {
      final var bucket = fakeDriver.bucketGet(bucketName);
      if (bucket != null) {
        final var accessorBucket = fromQueryParameter(bucketName, bucket.clientId(), AccessorStatus.READY);
        em.complete(accessorBucket);
      } else {
        em.fail(new CcsNotExistException(Response.Status.NOT_FOUND.getReasonPhrase()));
      }
    } catch (final DriverNotFoundException e) {
      throw new CcsNotExistException("Bucket doesn't exist");
    } catch (final DriverException e) {
      em.fail(new CcsOperationException(e.getMessage()));
    }
  }

  public static Uni<AccessorBucket> getBucket0Helper(final String bucketName, final String clientId) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        if (errorCode >= 400) {
          em.fail(CcsServerExceptionMapper.getCcsException(errorCode));
        } else {
          final var bucket = new AccessorBucket().setId(bucketName).setCreation(Instant.now().minusSeconds(100))
              .setSite(ServiceProperties.getAccessorSite()).setStatus(AccessorStatus.READY).setClientId(clientId);
          em.complete(bucket);
        }
      } else {
        getBucketHelper(em, bucketName);
      }
    });
  }

  public static void createBucketHelper(final UniEmitter<? super AccessorBucket> em, final String bucketName,
                                        final String clientId) {
    final var accessorBucket = fromQueryParameter(bucketName, clientId, AccessorStatus.READY);
    try (final var fakeDriver = DriverApiRegistry.getDriverApiFactory().getInstance()) {
      var storageBucket = new StorageBucket(accessorBucket.getId(), clientId, Instant.now());
      storageBucket = fakeDriver.bucketCreate(storageBucket);
      accessorBucket.setCreation(storageBucket.creationDate());
      em.complete(accessorBucket);
    } catch (final DriverNotAcceptableException e) {
      em.fail(new CcsNotAcceptableException(e.getMessage()));
    } catch (final DriverAlreadyExistException e) {
      em.fail(new CcsAlreadyExistException(e.getMessage()));
    } catch (final DriverException e) {
      em.fail(new CcsOperationException(e.getMessage()));
    }
  }

  public static Uni<AccessorBucket> createBucket0Helper(final String bucketName, final String clientId) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode >= 400) {
        em.fail(CcsServerExceptionMapper.getCcsException(errorCode));
      } else {
        createBucketHelper(em, bucketName, clientId);
      }
    });
  }

  public static void deleteBucketHelper(final UniEmitter<? super Response> em, final String bucketName,
                                        final String clientId) {
    try (final var fakeDriver = DriverApiRegistry.getDriverApiFactory().getInstance()) {
      final var bucket = fakeDriver.bucketGet(bucketName);
      if (bucket != null && !bucket.clientId().equals(clientId)) {
        throw new CcsNotAcceptableException(bucketName + " is not owned by current client");
      }
      fakeDriver.bucketDelete(bucketName);
      em.complete(Response.noContent().build());
    } catch (final DriverNotAcceptableException e) {
      em.fail(new CcsNotAcceptableException(e.getMessage()));
    } catch (final DriverNotFoundException e) {
      em.complete(Response.status(Response.Status.NOT_FOUND).build());
    } catch (final DriverException e) {
      em.fail(new CcsOperationException(e.getMessage()));
    }
  }

  public static Uni<Response> deleteBucket0Helper(final String bucketName, final String clientId) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        if (errorCode >= 400) {
          em.fail(CcsServerExceptionMapper.getCcsException(errorCode));
        } else {
          em.complete(Response.noContent().build());
        }
      } else {
        deleteBucketHelper(em, bucketName, clientId);
      }
    });
  }

  FakeCommonBucketResourceHelper() {
    // Empty
  }
}
