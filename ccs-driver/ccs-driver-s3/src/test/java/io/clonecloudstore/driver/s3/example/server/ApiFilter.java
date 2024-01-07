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

package io.clonecloudstore.driver.s3.example.server;

import java.util.Optional;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.core.MediaType;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.RestResponse;

/**
 * Uncomment annotations if you want to trace API calls from server side
 */
public class ApiFilter {
  private static final Logger LOG = Logger.getLogger(ApiFilter.class);

  //@ServerRequestFilter(preMatching = true)
  public void preMatchingFilter(ContainerRequestContext requestContext) {
    var builder = new StringBuilder("PREREQUEST: ");
    builder.append(requestContext.getMethod()).append(' ');
    try {
      builder.append(requestContext.getUriInfo().getRequestUri().toString());
    } catch (final RuntimeException e) {
      builder.append("URI not found");
    }
    builder.append(" Match: [");
    for (var uri : requestContext.getUriInfo().getMatchedURIs()) {
      builder.append(uri).append(" ");
    }
    builder.append("] Headers: ");
    for (var entry : requestContext.getHeaders().entrySet()) {
      builder.append(entry.getKey()).append("=[");
      for (var val : entry.getValue()) {
        builder.append(val).append(";");
      }
      builder.append("] ");
    }
    LOG.info(builder.toString());
  }

  //@ServerRequestFilter
  public Optional<RestResponse<Void>> getRequestFilter(ContainerRequestContext requestContext) {
    var builder = new StringBuilder("REQUEST: ");
    builder.append(requestContext.getMethod()).append(' ');
    try {
      builder.append(requestContext.getUriInfo().getRequestUri().toString());
    } catch (final RuntimeException e) {
      builder.append("URI not found");
    }
    builder.append(" Match: [");
    for (var uri : requestContext.getUriInfo().getMatchedURIs()) {
      builder.append(uri).append(" ");
    }
    builder.append("] Headers: ");
    for (var entry : requestContext.getHeaders().entrySet()) {
      builder.append(entry.getKey()).append("=[");
      for (var val : entry.getValue()) {
        builder.append(val).append(";");
      }
      builder.append("] ");
    }
    LOG.info(builder.toString());
    return Optional.empty();
  }

  //@ServerResponseFilter
  public void responseFilter(final ContainerResponseContext responseContext,
                             final ContainerRequestContext requestContext) {
    final var builder = new StringBuilder("RESPONSE: ");
    builder.append(requestContext.getMethod()).append(' ');
    try {
      builder.append(requestContext.getUriInfo().getRequestUri().toString());
    } catch (final RuntimeException e) {
      builder.append("URI not found");
    }
    builder.append(" Has Body: ").append(responseContext.hasEntity()).append(" Status: ")
        .append(responseContext.getStatus());
    builder.append(" Headers: ");
    for (final var entry : responseContext.getHeaders().entrySet()) {
      builder.append(entry.getKey()).append("=[");
      for (final var val : entry.getValue()) {
        builder.append(val).append(";");
      }
      builder.append("] ");
    }
    final var contentType = responseContext.getHeaderString("Content-Type");
    final var isJson = contentType != null && contentType.startsWith(MediaType.APPLICATION_JSON);
    builder.append("Content ? ").append(responseContext.hasEntity()).append(" Json? ").append(isJson);
    if (responseContext.hasEntity() && isJson) {
      builder.append(" Content = ").append(responseContext.getEntity());
    }
    LOG.info(builder.toString());
  }
}
