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

package io.clonecloudstore.driver.s3.example.client;

import java.io.Closeable;
import java.io.InputStream;
import java.util.List;

import io.clonecloudstore.common.quarkus.client.ClientResponseExceptionMapper;
import io.clonecloudstore.common.quarkus.client.RequestHeaderFactory;
import io.clonecloudstore.driver.api.model.StorageBucket;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.rest.client.annotation.RegisterClientHeaders;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import org.jboss.resteasy.reactive.NoCache;
import org.jboss.resteasy.reactive.RestHeader;
import org.jboss.resteasy.reactive.RestPath;

@Path(ApiConstants.API_ROOT)
@RegisterRestClient
@RegisterProvider(ClientResponseExceptionMapper.class)
@RegisterProvider(ResponseObjectClientFilter.class)
@RegisterClientHeaders(RequestHeaderFactory.class)
@NoCache
public interface ApiServiceInterface extends Closeable {

  @Path("/")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  Uni<List<StorageBucket>> bucketList();

  @Path("/{bucket}")
  @HEAD
  Uni<Response> bucketExists(@RestPath String bucket);

  @Path("/{bucket}")
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  Uni<StorageBucket> bucketCreate(@RestPath String bucket);

  @Path("/{bucket}")
  @DELETE
  Uni<Response> bucketDelete(@RestPath String bucket);

  @Path("/{bucket}/{objectOrDirectory:.+}")
  @HEAD
  Uni<Response> objectOrDirectoryExists(@RestPath("bucket") String bucket,
                                        @RestPath("objectOrDirectory") String objectOrDirectory);

  @Path("/{bucket}/{object:.+}")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  Uni<StorageObject> getObjectMetadata(@RestPath("bucket") String bucket, @RestPath("object") String object);

  @Path("/{bucket}/{object:.+}")
  @DELETE
  Uni<Response> objectDelete(@RestPath("bucket") String bucket, @RestPath("object") String object);

  @Path("/{bucket}/{object:.+}")
  @POST
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  @Produces(MediaType.APPLICATION_JSON)
  Uni<Response> createObject(@RestPath("bucket") final String bucket,
                             @RestPath("object") final String objectOrDirectory,
                             @DefaultValue("0") @RestHeader(ApiConstants.X_LEN) final long len,
                             @DefaultValue("") @RestHeader(ApiConstants.X_HASH) final String hash,
                             final InputStream inputStream);

  @Path("/{bucket}/{object:.+}")
  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  Uni<InputStream> readObject(@RestPath("bucket") final String bucket,
                              @RestPath("object") final String objectOrDirectory);
}
