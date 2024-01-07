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

package io.clonecloudstore.common.quarkus.exception;

import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.client.api.WebClientApplicationException;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

import static io.clonecloudstore.common.quarkus.client.SimpleClientAbstract.X_ERROR;

/**
 * Global Server Exception Mapper
 */
public class CcsServerGenericExceptionMapper {
  private static final Logger LOGGER = Logger.getLogger(CcsServerGenericExceptionMapper.class);
  public static final String EXCEPTION_MAPPER = "Exception Mapper : %s - %s - %s (%s)";

  public static RuntimeException getCcsException(final int status) {
    return getCcsException(status, null, null);
  }

  public static RuntimeException getCcsException(final int status, final String message, final Throwable throwable) {
    final var realStatus = Response.Status.fromStatusCode(status);
    final var realMsg = message == null ? realStatus.getReasonPhrase() : message;
    if (status < Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()) {
      return switch (status) {
        case 404 ->
            (throwable == null) ? new CcsNotExistException(realMsg) : new CcsNotExistException(realMsg, throwable);
        case 406 -> (throwable == null) ? new CcsNotAcceptableException(realMsg) :
            new CcsNotAcceptableException(realMsg, throwable);
        case 409 -> (throwable == null) ? new CcsAlreadyExistException(realMsg) :
            new CcsAlreadyExistException(realMsg, throwable);
        case 410 ->
            (throwable == null) ? new CcsDeletedException(realMsg) : new CcsDeletedException(realMsg, throwable);
        default -> (throwable == null) ? new CcsClientGenericException(realMsg, realStatus) :
            new CcsClientGenericException(realMsg, realStatus, throwable);
      };
    } else {
      if (status == 500) {
        return (throwable == null) ? new CcsOperationException(realMsg) : new CcsOperationException(realMsg, throwable);
      }
      return (throwable == null) ? new CcsServerGenericException(realMsg, realStatus) :
          new CcsServerGenericException(realMsg, realStatus, throwable);
    }
  }

  public static CcsWithStatusException getBusinessException(final WebApplicationException e) {
    return new CcsWithStatusException("API", e.getResponse().getStatus(), e);
  }

  /**
   * Exception Server Handler
   *
   * @param exception CcsServerGenericException
   * @return Uni Response
   */
  @ServerExceptionMapper({CcsServerGenericException.class})
  public Uni<Response> handleServerException(final CcsServerGenericException exception) {
    LOGGER.errorf(EXCEPTION_MAPPER, exception.getMessage(), exception.getStatus(), exception.getCause(),
        exception.getClass().getSimpleName(), exception);
    return Uni.createFrom().emitter(
        em -> em.complete(Response.status(exception.getStatus()).header(X_ERROR, exception.getMessage()).build()));
  }

  @ServerExceptionMapper({CcsClientGenericException.class})
  public Uni<Response> handleClientException(final CcsClientGenericException exception) {
    LOGGER.errorf(EXCEPTION_MAPPER, exception.getMessage(), exception.getStatus(), exception.getCause(),
        exception.getClass().getSimpleName(), exception);
    return Uni.createFrom().emitter(
        em -> em.complete(Response.status(exception.getStatus()).header(X_ERROR, exception.getMessage()).build()));
  }

  @ServerExceptionMapper({WebClientApplicationException.class})
  public Uni<Response> handleClientException(final WebClientApplicationException exception) {
    LOGGER.errorf(EXCEPTION_MAPPER, exception.getMessage(), exception.getResponse().getStatus(), exception.getCause(),
        exception.getClass().getSimpleName(), exception);
    return Uni.createFrom().emitter(em -> em.complete(
        Response.status(exception.getResponse().getStatus()).header(X_ERROR, exception.getMessage()).build()));
  }
}
