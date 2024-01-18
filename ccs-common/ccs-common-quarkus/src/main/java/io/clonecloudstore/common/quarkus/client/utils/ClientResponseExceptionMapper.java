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

package io.clonecloudstore.common.quarkus.client.utils;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import io.clonecloudstore.common.quarkus.client.SimpleClientAbstract;
import io.clonecloudstore.common.quarkus.exception.CcsClientGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.inputstream.MultipleActionsInputStream;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.quarkus.arc.Unremovable;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import org.eclipse.microprofile.rest.client.ext.ResponseExceptionMapper;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.ClientWebApplicationException;

import static io.clonecloudstore.common.standard.properties.ApiConstants.X_ERROR;

/**
 * Implementation for a @Provider for handling exceptions in case of POST/GET InputStream
 */
//@Provider
@ApplicationScoped
@Unremovable
public class ClientResponseExceptionMapper implements ResponseExceptionMapper<RuntimeException> {
  private static final Logger LOGGER = Logger.getLogger(ClientResponseExceptionMapper.class);
  static final String NO_RESPONSE = "No Response";
  static final String RESPONSE_ISSUE = "Response issue: ";

  /**
   * Web Application Exception (Runtime) to Ccs With Status Exception (Exception)
   */
  public static CcsWithStatusException getBusinessException(final WebApplicationException e) {
    if (e.getCause() instanceof CcsClientGenericException ccge) {
      if (ccge.getCause() instanceof CcsWithStatusException cwse) {
        return cwse;
      }
      return new CcsWithStatusException("API", ccge.getStatus(), ccge);
    }
    return new CcsWithStatusException("API", e.getResponse().getStatus(), e);
  }

  /**
   * Default handling for Uni Response (not for InputStream)
   *
   * @param uni the Uni response
   * @return the Response if no error
   */
  public Response handleUniResponse(final Uni<Response> uni) throws CcsWithStatusException {
    try (final var response = uni.ifNoItem().after(QuarkusProperties.getDurationResponseTimeout()).fail().await()
        .atMost(QuarkusProperties.getDurationResponseTimeout())) {
      if (response == null) {
        // Issue
        throw new CcsClientGenericException(NO_RESPONSE, Status.PRECONDITION_FAILED);
      }
      responseToExceptionIfError(response);
      return response;
    } catch (final CcsClientGenericException | CcsServerGenericException | ClientWebApplicationException e) {
      throw getBusinessException(e);
    } catch (final RuntimeException e) {
      LOGGER.info(e);
      throw new CcsWithStatusException(null, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          RESPONSE_ISSUE + e.getMessage(), e);
    }
  }

  public Response handleCompletableResponse(final CompletableFuture<Response> completableFuture)
      throws CcsWithStatusException {
    try (final var response = completableFuture.get()) {
      if (response == null) {
        // Issue
        throw new CcsClientGenericException(NO_RESPONSE, Status.PRECONDITION_FAILED);
      }
      responseToExceptionIfError(response);
      return response;
    } catch (final CcsClientGenericException | CcsServerGenericException | ClientWebApplicationException e) {
      throw getBusinessException(e);
    } catch (final ExecutionException e) {
      if (e.getCause() instanceof WebApplicationException wae) {
        throw getBusinessException(wae);
      }
      throw new CcsWithStatusException(null, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          RESPONSE_ISSUE + e.getMessage(), e);
    } catch (final RuntimeException | InterruptedException e) { // NOSONAR intentional
      LOGGER.info(e);
      throw new CcsWithStatusException(null, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          RESPONSE_ISSUE + e.getMessage(), e);
    }
  }

  /**
   * Generate a CcsWithStatusException if needed
   */
  public void responseToExceptionIfError(final Response response) throws CcsWithStatusException {
    final var status = response.getStatus();
    if (status >= Status.BAD_REQUEST.getStatusCode()) {
      final var generic = Status.fromStatusCode(status);
      var message = response.getHeaderString(X_ERROR);
      if (ParametersChecker.isEmpty(message)) {
        message = response.hasEntity() && response.bufferEntity() ? response.readEntity(String.class) :
            generic.getReasonPhrase();
      }
      throw new CcsWithStatusException(message, status);
    }
  }

  public Object handleUniObject(final SimpleClientAbstract<?> simpleClientAbstract, final Uni<?> uni)
      throws CcsWithStatusException {
    return handleUniObject(simpleClientAbstract, uni, null);
  }

  public Object handleUniObject(final SimpleClientAbstract<?> simpleClientAbstract, final Uni<?> uni,
                                final InputStream inputStreamOptional) throws CcsWithStatusException {
    try {
      final var response = uni.ifNoItem().after(QuarkusProperties.getDurationResponseTimeout()).fail().await()
          .atMost(QuarkusProperties.getDurationResponseTimeout());
      if (response == null) {
        // Issue
        throw new CcsClientGenericException(NO_RESPONSE, Status.PRECONDITION_FAILED);
      }
      if (inputStreamOptional instanceof MultipleActionsInputStream mai) {
        LOGGER.debugf("DEBUG %s", mai);
      }
      return response;
    } catch (final CcsClientGenericException | CcsServerGenericException | ClientWebApplicationException e) {
      if (inputStreamOptional != null) {
        SystemTools.consumeWhileErrorInputStream(inputStreamOptional, StandardProperties.getMaxWaitMs());
      }
      simpleClientAbstract.reopen();
      throw getBusinessException(e);
    } catch (final RuntimeException e) {
      LOGGER.info(e);
      if (inputStreamOptional != null) {
        SystemTools.consumeWhileErrorInputStream(inputStreamOptional, StandardProperties.getMaxWaitMs());
      }
      simpleClientAbstract.reopen();
      throw new CcsWithStatusException(null, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          RESPONSE_ISSUE + e.getMessage(), e);
    }
  }

  public Object handleCompletableObject(final SimpleClientAbstract<?> simpleClientAbstract,
                                        final CompletableFuture<?> completableFuture) throws CcsWithStatusException {
    try {
      final var response = completableFuture.get();
      if (response == null) {
        // Issue
        throw new CcsClientGenericException(NO_RESPONSE, Status.PRECONDITION_FAILED);
      }
      return response;
    } catch (final CcsClientGenericException | CcsServerGenericException | ClientWebApplicationException e) {
      simpleClientAbstract.reopen();
      throw getBusinessException(e);
    } catch (final ExecutionException e) {
      simpleClientAbstract.reopen();
      if (e.getCause() instanceof WebApplicationException wae) {
        throw getBusinessException(wae);
      }
      throw new CcsWithStatusException(null, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          RESPONSE_ISSUE + e.getMessage(), e);
    } catch (final RuntimeException | InterruptedException e) { // NOSONAR intentional
      LOGGER.info(e);
      simpleClientAbstract.reopen();
      throw new CcsWithStatusException(null, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          RESPONSE_ISSUE + e.getMessage(), e);
    }
  }

  @Override
  public RuntimeException toThrowable(final Response response) {
    try {
      responseToExceptionIfError(response);
      return null;
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() < Status.INTERNAL_SERVER_ERROR.getStatusCode()) {
        return new CcsClientGenericException(e.getMessage(), Status.fromStatusCode(e.getStatus()), e);
      }
      return new CcsServerGenericException(e.getMessage(), Status.fromStatusCode(e.getStatus()), e);
    }
  }

  @Override
  public boolean handles(final int status, final MultivaluedMap<String, Object> headers) {
    return status >= Status.BAD_REQUEST.getStatusCode();
  }

  @Override
  public int getPriority() {
    // High priority
    return 10;
  }
}
