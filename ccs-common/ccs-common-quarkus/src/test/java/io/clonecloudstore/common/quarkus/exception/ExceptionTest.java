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

import java.util.List;

import io.clonecloudstore.common.standard.properties.Module;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

@QuarkusTest
class ExceptionTest {
  @Test
  void testCcsServerException() {
    CcsServerGenericException serverInternalEx = new CcsOperationException("Internal Error Exception");
    assertEquals(Module.UNKNOWN + " - Internal Error Exception", serverInternalEx.getMessage());
    assertEquals(500, serverInternalEx.getResponse().getStatus());
    assertEquals(Module.UNKNOWN, serverInternalEx.getModule());

    CcsServerGenericException serverInternalWithCauseEx =
        new CcsOperationException("Internal Error Exception with cause", serverInternalEx);
    assertEquals(Module.UNKNOWN + " - Internal Error Exception with cause", serverInternalWithCauseEx.getMessage());
    assertEquals(Module.UNKNOWN + " - Internal Error Exception", serverInternalWithCauseEx.getCause().getMessage());
    assertEquals(Module.UNKNOWN, serverInternalWithCauseEx.getModule());
    assertEquals(500, serverInternalWithCauseEx.getResponse().getStatus());

    //Test generic server error exception mapper
    //Consume Uni<Response> and use response to check status and error message
    CcsServerGenericExceptionMapper exceptionMapper = new CcsServerGenericExceptionMapper();
    Uni<Response> responseExceptionMapper = exceptionMapper.handleServerException(serverInternalEx);
    Response response =
        responseExceptionMapper.subscribe().withSubscriber(UniAssertSubscriber.create()).assertCompleted().getItem();
    assertEquals(500, response.getStatus());
    assertEquals("Internal Server Error", response.getStatusInfo().getReasonPhrase());
  }

  @Test
  void testCcsClientException() {
    CcsAlreadyExistException alreadyExistException = new CcsAlreadyExistException("Already Exist Exception");
    assertEquals(Module.UNKNOWN + " - Already Exist Exception", alreadyExistException.getMessage());
    assertEquals(409, alreadyExistException.getResponse().getStatus());
    assertEquals(Module.UNKNOWN, alreadyExistException.getModule());

    CcsDeletedException deletedException = new CcsDeletedException("Deleted Exception");
    assertEquals(Module.UNKNOWN + " - Deleted Exception", deletedException.getMessage());
    assertEquals(410, deletedException.getResponse().getStatus());
    assertEquals(Module.UNKNOWN, deletedException.getModule());

    CcsNotAcceptableException notAcceptableException = new CcsNotAcceptableException("Not Acceptable Exception");
    assertEquals(Module.UNKNOWN + " - Not Acceptable Exception", notAcceptableException.getMessage());
    assertEquals(406, notAcceptableException.getResponse().getStatus());
    assertEquals(Module.UNKNOWN, notAcceptableException.getModule());

    CcsNotAcceptableException notAcceptableException2 =
        new CcsNotAcceptableException("Not Acceptable Exception2", deletedException);
    assertEquals(Module.UNKNOWN + " - Not Acceptable Exception2", notAcceptableException2.getMessage());
    assertEquals(406, notAcceptableException2.getResponse().getStatus());
    assertEquals(Module.UNKNOWN, notAcceptableException2.getModule());

    CcsNotExistException notExistException = new CcsNotExistException("Not Exist Exception");
    assertEquals(Module.UNKNOWN + " - Not Exist Exception", notExistException.getMessage());
    assertEquals(404, notExistException.getResponse().getStatus());
    assertEquals(Module.UNKNOWN, notExistException.getModule());

    CcsNotExistException notExistExceptionWithCause =
        new CcsNotExistException("Not Exist Exception with cause", notExistException);
    assertEquals(Module.UNKNOWN + " - Not Exist Exception with cause", notExistExceptionWithCause.getMessage());
    assertEquals(Module.UNKNOWN + " - Not Exist Exception", notExistExceptionWithCause.getCause().getMessage());
    assertEquals(404, notExistExceptionWithCause.getResponse().getStatus());
    assertEquals(Module.UNKNOWN, notExistExceptionWithCause.getModule());

    CcsOperationException operationException = new CcsOperationException("Operation Exception");
    assertEquals(Module.UNKNOWN + " - Operation Exception", operationException.getMessage());
    assertEquals(500, operationException.getResponse().getStatus());
    assertEquals(Module.UNKNOWN, operationException.getModule());

    CcsOperationException operationExceptionWithCause =
        new CcsOperationException("Operation Exception with cause", operationException);
    assertEquals(Module.UNKNOWN + " - Operation Exception with cause", operationExceptionWithCause.getMessage());
    assertEquals(500, operationExceptionWithCause.getResponse().getStatus());
    assertEquals(Module.UNKNOWN + " - Operation Exception", operationExceptionWithCause.getCause().getMessage());
    assertEquals(Module.UNKNOWN, operationExceptionWithCause.getModule());

    CcsOperationException operationExceptionWithCause2 = new CcsOperationException(operationException);
    assertEquals(Module.UNKNOWN + " - HTTP 500 " + Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase(),
        operationExceptionWithCause2.getMessage());
    assertEquals(500, operationExceptionWithCause2.getResponse().getStatus());
    assertEquals(Module.UNKNOWN + " - Operation Exception", operationExceptionWithCause2.getCause().getMessage());
    assertEquals(Module.UNKNOWN, operationExceptionWithCause2.getModule());
  }

  @Test
  void testExceptionMapper() {
    final var exception = new Exception("test");
    final var listStatus = List.of(Response.Status.NOT_FOUND, Response.Status.CONFLICT, Response.Status.GONE,
        Response.Status.NOT_ACCEPTABLE, Response.Status.REQUEST_ENTITY_TOO_LARGE);
    for (final var status : listStatus) {
      var e = CcsServerGenericExceptionMapper.getCcsException(status.getStatusCode());
      assertEquals(status.getStatusCode(), ((CcsClientGenericException) e).getStatus());
      assertEquals(Module.UNKNOWN + " - " + status.getReasonPhrase(), e.getMessage());
      assertNull(e.getCause());
      e = CcsServerGenericExceptionMapper.getCcsException(status.getStatusCode(), "message", null);
      assertEquals(status.getStatusCode(), ((CcsClientGenericException) e).getStatus());
      assertEquals(Module.UNKNOWN + " - " + "message", e.getMessage());
      assertNull(e.getCause());
      e = CcsServerGenericExceptionMapper.getCcsException(status.getStatusCode(), "message", exception);
      assertEquals(status.getStatusCode(), ((CcsClientGenericException) e).getStatus());
      assertEquals(Module.UNKNOWN + " - " + "message", e.getMessage());
      assertEquals(exception, e.getCause());
      switch (status) {
        case NOT_FOUND -> assertInstanceOf(CcsNotExistException.class, e);
        case CONFLICT -> assertInstanceOf(CcsAlreadyExistException.class, e);
        case GONE -> assertInstanceOf(CcsDeletedException.class, e);
        case NOT_ACCEPTABLE -> assertInstanceOf(CcsNotAcceptableException.class, e);
        default -> assertInstanceOf(CcsClientGenericException.class, e);
      }
    }
    final var listStatusServer = List.of(Response.Status.INTERNAL_SERVER_ERROR, Response.Status.NOT_IMPLEMENTED);
    for (final var status : listStatusServer) {
      var e = CcsServerGenericExceptionMapper.getCcsException(status.getStatusCode());
      assertEquals(status.getStatusCode(), ((CcsServerGenericException) e).getStatus());
      assertEquals(Module.UNKNOWN + " - " + status.getReasonPhrase(), e.getMessage());
      assertNull(e.getCause());
      e = CcsServerGenericExceptionMapper.getCcsException(status.getStatusCode(), "message", null);
      assertEquals(status.getStatusCode(), ((CcsServerGenericException) e).getStatus());
      assertEquals(Module.UNKNOWN + " - " + "message", e.getMessage());
      assertNull(e.getCause());
      e = CcsServerGenericExceptionMapper.getCcsException(status.getStatusCode(), "message", exception);
      assertEquals(status.getStatusCode(), ((CcsServerGenericException) e).getStatus());
      assertEquals(Module.UNKNOWN + " - " + "message", e.getMessage());
      assertEquals(exception, e.getCause());
      switch (status) {
        case INTERNAL_SERVER_ERROR -> assertInstanceOf(CcsOperationException.class, e);
        default -> assertInstanceOf(CcsServerGenericException.class, e);
      }
    }
  }
}
