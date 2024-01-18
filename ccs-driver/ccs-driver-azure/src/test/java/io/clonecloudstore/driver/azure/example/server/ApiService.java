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

package io.clonecloudstore.driver.azure.example.server;

import java.io.InputStream;
import java.util.List;

import io.clonecloudstore.common.quarkus.exception.CcsAlreadyExistException;
import io.clonecloudstore.common.quarkus.exception.CcsNotAcceptableException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.server.service.StreamServiceAbstract;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.DriverApiFactory;
import io.clonecloudstore.driver.api.DriverApiRegistry;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.driver.api.exception.DriverAlreadyExistException;
import io.clonecloudstore.driver.api.exception.DriverException;
import io.clonecloudstore.driver.api.exception.DriverNotAcceptableException;
import io.clonecloudstore.driver.api.exception.DriverNotFoundException;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.clonecloudstore.driver.azure.example.client.ApiConstants;
import io.quarkus.resteasy.reactive.server.Closer;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.RestHeader;
import org.jboss.resteasy.reactive.RestPath;

import static io.clonecloudstore.common.standard.properties.ApiConstants.X_ERROR;

@Path(ApiConstants.API_ROOT)
public class ApiService extends StreamServiceAbstract<StorageObject, StorageObject, StreamHandler> {
  public static final String NOT_ACCEPTABLE_NAME = "notAcceptableName";
  private final DriverApiFactory driverApiFactory;
  private static final Logger LOG = Logger.getLogger(ApiService.class);


  public ApiService(final DriverApiFactory driverApiFactory) {
    super();
    this.driverApiFactory = DriverApiRegistry.getDriverApiFactory();
  }

  @Path("/")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  public Uni<List<StorageBucket>> bucketList() {
    return Uni.createFrom().emitter(em -> {
      try (final var driverApi = driverApiFactory.getInstance()) {
        final var stream = driverApi.bucketsStream();
        em.complete(stream.toList());
      } catch (final DriverException e) {
        LOG.error(e.getMessage(), e);
        throw new CcsOperationException(e);
      }
    });
  }

  @Path("/{bucket}")
  @HEAD
  @Blocking
  public Uni<Response> bucketExists(@RestPath("bucket") final String bucket) {
    return Uni.createFrom().emitter(em -> {
      try (final var driverApi = driverApiFactory.getInstance()) {
        final var found = driverApi.bucketExists(bucket);
        if (found) {
          em.complete(Response.status(Response.Status.OK).header(ApiConstants.X_BUCKET, bucket)
              .header(ApiConstants.X_TYPE, StorageType.BUCKET.name()).build());
        } else {
          em.complete(Response.status(Response.Status.NOT_FOUND).header(ApiConstants.X_BUCKET, bucket)
              .header(ApiConstants.X_TYPE, StorageType.NONE.name()).build());
        }

      } catch (final DriverException e) {
        LOG.error(e.getMessage(), e);
        em.complete(Response.status(Response.Status.INTERNAL_SERVER_ERROR).header(X_ERROR, e.getMessage()).build());
      }
    });
  }

  @Path("/{bucket}")
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  public Uni<StorageBucket> bucketCreate(@RestPath("bucket") final String bucket) {
    return Uni.createFrom().emitter(em -> {
      try (final var driverApi = driverApiFactory.getInstance()) {
        final var bucket1 = new StorageBucket(bucket, "client", null);
        final var storageBucket = driverApi.bucketCreate(bucket1);
        em.complete(storageBucket);
      } catch (final DriverNotAcceptableException e) {
        LOG.debug(e.getMessage());
        em.fail(new CcsNotAcceptableException(e.getMessage(), e));
      } catch (final DriverAlreadyExistException e) {
        LOG.debug(e.getMessage());
        em.fail(new CcsAlreadyExistException(e.getMessage(), e));
      } catch (final DriverException e) {
        LOG.error(e.getMessage(), e);
        em.fail(new CcsOperationException(e));
      }
    });
  }

  @Path("/{bucket}")
  @DELETE
  @Blocking
  public Uni<Response> bucketDelete(@RestPath("bucket") final String bucket) {
    return Uni.createFrom().emitter(em -> {
      try (final var driverApi = driverApiFactory.getInstance()) {
        driverApi.bucketDelete(bucket);
        em.complete(Response.status(Response.Status.OK).header(ApiConstants.X_BUCKET, bucket).build());
      } catch (final DriverNotFoundException e) {
        LOG.debug(e.getMessage());
        em.complete(Response.status(Response.Status.NOT_FOUND).header(ApiConstants.X_BUCKET, bucket).build());
      } catch (final DriverNotAcceptableException e) {
        LOG.debug(e.getMessage());
        em.complete(Response.status(Response.Status.NOT_ACCEPTABLE).header(ApiConstants.X_BUCKET, bucket).build());
      } catch (final DriverException e) {
        LOG.error(e.getMessage(), e);
        em.complete(Response.status(Response.Status.INTERNAL_SERVER_ERROR).header(X_ERROR, e.getMessage()).build());
      }
    });
  }

  @Path("/{bucket}/{objectOrDirectory:.+}")
  @HEAD
  @Blocking
  public Uni<Response> objectOrDirectoryExists(@RestPath("bucket") final String bucket,
                                               @RestPath("objectOrDirectory") final String objectOrDirectory) {
    final var decodedName = ParametersChecker.urlDecodePathParam(objectOrDirectory);
    return Uni.createFrom().emitter(em -> {
      try (final var driverApi = driverApiFactory.getInstance()) {
        final var storageType = driverApi.directoryOrObjectExistsInBucket(bucket, decodedName);
        em.complete(Response.status(Response.Status.OK).header(ApiConstants.X_TYPE, storageType.name()).build());
      } catch (final DriverException e) {
        LOG.error(e.getMessage(), e);
        em.complete(Response.status(Response.Status.INTERNAL_SERVER_ERROR).header(X_ERROR, e.getMessage()).build());
      }
    });
  }


  @Path("/{bucket}/{object:.+}")
  @DELETE
  @Blocking
  public Uni<Response> objectDelete(@RestPath("bucket") final String bucket,
                                    @RestPath("object") final String objectOrDirectory) {
    final var decodedName = ParametersChecker.urlDecodePathParam(objectOrDirectory);
    return Uni.createFrom().emitter(em -> {
      try (final var driverApi = driverApiFactory.getInstance()) {
        driverApi.objectDeleteInBucket(bucket, decodedName);
        em.complete(Response.status(Response.Status.OK).header(ApiConstants.X_BUCKET, bucket)
            .header(ApiConstants.X_OBJECT, decodedName).build());
      } catch (final DriverNotFoundException e) {
        LOG.debug(e.getMessage());
        em.complete(Response.status(Response.Status.NOT_FOUND).header(ApiConstants.X_BUCKET, bucket)
            .header(ApiConstants.X_OBJECT, decodedName).build());
      } catch (final DriverNotAcceptableException e) {
        LOG.debug(e.getMessage());
        em.complete(Response.status(Response.Status.CONFLICT).header(ApiConstants.X_BUCKET, bucket)
            .header(ApiConstants.X_OBJECT, decodedName).build());
      } catch (final DriverException e) {
        LOG.error(e.getMessage(), e);
        em.complete(Response.status(Response.Status.INTERNAL_SERVER_ERROR).header(X_ERROR, e.getMessage()).build());
      }
    });
  }

  // REST API for receiving InputStream from client
  @Path("/{bucket}/{object:.+}")
  @POST
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  public Uni<Response> createObject(final HttpServerRequest request, @Context Closer closer,
                                    @RestPath("bucket") final String bucket,
                                    @RestPath("object") final String objectOrDirectory,
                                    @DefaultValue("0") @RestHeader(ApiConstants.X_LEN) final long len,
                                    @DefaultValue("") @RestHeader(ApiConstants.X_HASH) final String hash,
                                    final InputStream inputStream) {
    // Business code should come here
    final var decodedName = ParametersChecker.urlDecodePathParam(objectOrDirectory);
    final var sHash = ParametersChecker.isEmpty(hash) ? null : hash;
    final var businessIn = new StorageObject(bucket, decodedName, sHash, len, null);
    // use InputStream abstract implementation
    return createObject(request, closer, businessIn, businessIn.size(), businessIn.hash(), inputStream);
  }

  // REST API for sending InputStream back to client
  @Path("/{bucket}/{object:.+}")
  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Blocking
  public Uni<Response> readObject(final HttpServerRequest request, @Context final Closer closer,
                                  @RestPath("bucket") final String bucket,
                                  @RestPath("object") final String objectOrDirectory,
                                  @DefaultValue("0") @RestHeader(ApiConstants.X_LEN) final long len) {
    final var decodedName = ParametersChecker.urlDecodePathParam(objectOrDirectory);
    // Business code should come here
    final var businessIn = new StorageObject(bucket, decodedName, null, len, null);
    // use InputStream abstract implementation
    return readObject(request, closer, businessIn, false);
  }

  @Path("/{bucket}/{object:.+}")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Blocking
  public Uni<StorageObject> getObjectMetadata(@RestPath("bucket") final String bucket,
                                              @RestPath("object") final String objectOrDirectory) {
    final var decodedName = ParametersChecker.urlDecodePathParam(objectOrDirectory);
    return Uni.createFrom().emitter(em -> {
      // Business code should come here
      try (final var driverApi = driverApiFactory.getInstance()) {
        final var storageObject = driverApi.objectGetMetadataInBucket(bucket, decodedName);
        em.complete(storageObject);
      } catch (final DriverNotFoundException e) {
        LOG.debug(e.getMessage());
        em.fail(new NotFoundException(e));
      } catch (final DriverException e) {
        LOG.error(e.getMessage(), e);
        em.fail(new CcsOperationException(e));
      }
    });
  }
}
