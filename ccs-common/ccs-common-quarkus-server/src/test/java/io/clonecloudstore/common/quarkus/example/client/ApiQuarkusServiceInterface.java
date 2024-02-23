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

package io.clonecloudstore.common.quarkus.example.client;

import java.io.Closeable;
import java.io.InputStream;

import io.clonecloudstore.common.quarkus.client.utils.ClientResponseExceptionMapper;
import io.clonecloudstore.common.quarkus.client.utils.RequestHeaderFactory;
import io.clonecloudstore.common.quarkus.example.model.ApiBusinessOut;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.rest.client.annotation.RegisterClientHeaders;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import org.jboss.resteasy.reactive.NoCache;

import static io.clonecloudstore.common.quarkus.example.client.ApiConstants.API_COLLECTIONS;
import static io.clonecloudstore.common.quarkus.example.client.ApiConstants.THROUGH;
import static io.clonecloudstore.common.quarkus.example.client.ApiConstants.X_LEN;
import static io.clonecloudstore.common.quarkus.example.client.ApiConstants.X_NAME;

@RegisterRestClient
@Path(ApiConstants.API_FULLROOT)
@RegisterProvider(ClientResponseExceptionMapper.class)
@RegisterProvider(ResponseClientFilter.class)
@RegisterClientHeaders(RequestHeaderFactory.class)
@NoCache
public interface ApiQuarkusServiceInterface extends Closeable {

  @Path(ApiConstants.API_COLLECTIONS + "/{name}")
  @HEAD
  Uni<Response> checkName(@PathParam("name") String name);

  @Path(ApiConstants.API_COLLECTIONS + "/{business}")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  Uni<ApiBusinessOut> getObjectMetadata(@PathParam("business") String business);

  @Path(API_COLLECTIONS)
  @POST
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  @Produces(MediaType.APPLICATION_JSON)
  Uni<Response> createObject(@DefaultValue("default-name") @HeaderParam(X_NAME) final String name,
                             @DefaultValue("0") @HeaderParam(X_LEN) final long len, final InputStream inputStream);

  @Path(API_COLLECTIONS)
  @PUT
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  @Produces(MediaType.APPLICATION_JSON)
  Uni<Response> createObjectUsingPut(@DefaultValue("default-name") @HeaderParam(X_NAME) final String name,
                                     @DefaultValue("0") @HeaderParam(X_LEN) final long len,
                                     final InputStream inputStream);

  @Path(API_COLLECTIONS + THROUGH)
  @POST
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  @Produces(MediaType.APPLICATION_JSON)
  Uni<Response> createObjectThrough(@DefaultValue("default-name") @HeaderParam(X_NAME) final String name,
                                    @DefaultValue("0") @HeaderParam(X_LEN) final long len,
                                    final InputStream inputStream);

  @Path(API_COLLECTIONS + "/{business}")
  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  Uni<InputStream> readObject(@PathParam("business") final String business);

  @Path(API_COLLECTIONS + "/{business}")
  @PUT
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  Uni<InputStream> readObjectPut(@PathParam("business") final String business);

  @Path(API_COLLECTIONS + THROUGH + "/{business}")
  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  Uni<InputStream> readObjectThrough(@PathParam("business") final String business);
}
