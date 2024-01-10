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

package io.clonecloudstore.common.quarkus.client.example;

import java.io.Closeable;
import java.io.InputStream;

import io.clonecloudstore.common.quarkus.client.example.model.ApiBusinessOut;
import io.clonecloudstore.common.quarkus.client.utils.ClientResponseExceptionMapper;
import io.clonecloudstore.common.quarkus.client.utils.RequestHeaderFactory;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.Consumes;
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
import org.jboss.resteasy.reactive.RestQuery;

import static io.clonecloudstore.common.quarkus.client.example.ApiConstants.API_COLLECTIONS;
import static io.clonecloudstore.common.quarkus.client.example.ApiConstants.API_ROOT;
import static io.clonecloudstore.common.quarkus.client.example.ApiConstants.X_LEN;
import static io.clonecloudstore.common.quarkus.client.example.ApiConstants.X_NAME;

@Path(API_ROOT)
@RegisterRestClient
@RegisterProvider(ClientResponseExceptionMapper.class)
@RegisterClientHeaders(RequestHeaderFactory.class)
@RegisterProvider(ResponseClientFilter.class)
@NoCache
public interface ApiServiceInterface extends Closeable {
  @Path(API_COLLECTIONS + "/{business}")
  @HEAD
  Uni<Response> checkName(@RestPath String business, @RestQuery("length") long length,
                          @RestHeader("x-length") long xlength, @RestHeader("x-slength") String xslength);

  @Path(API_COLLECTIONS + "/{business}")
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  Uni<ApiBusinessOut> getObjectMetadata(@RestPath String business);

  @Path(API_COLLECTIONS)
  @POST
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  @Produces(MediaType.APPLICATION_JSON)
  Uni<Response> createObject(@DefaultValue("name") @RestHeader(X_NAME) final String name,
                             @DefaultValue("0") @RestHeader(X_LEN) final long len, final InputStream inputStream);

  @Path(API_COLLECTIONS + "/{business}")
  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  Uni<InputStream> readObject(@RestPath final String business, @DefaultValue("0") @RestHeader(X_LEN) final long len);

}
