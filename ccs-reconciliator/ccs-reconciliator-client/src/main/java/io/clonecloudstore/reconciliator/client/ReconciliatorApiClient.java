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

package io.clonecloudstore.reconciliator.client;

import java.util.Iterator;
import java.util.Map;

import io.clonecloudstore.common.quarkus.client.ClientAbstract;
import io.clonecloudstore.common.quarkus.client.utils.ClientResponseExceptionMapper;
import io.clonecloudstore.common.quarkus.exception.CcsClientGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.stream.StreamIteratorUtils;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.reconciliator.client.api.ReconciliatorApi;
import io.clonecloudstore.reconciliator.model.ReconciliationRequest;
import io.clonecloudstore.reconciliator.model.ReconciliationSitesAction;
import io.clonecloudstore.reconciliator.model.ReconciliationSitesListing;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.ClientWebApplicationException;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.X_REQUEST_ID;

/**
 * Client for Reconciliator
 */
public class ReconciliatorApiClient
    extends ClientAbstract<ReconciliationRequest, ReconciliationRequest, ReconciliatorApi> {
  private static final Logger LOGGER = Logger.getLogger(ReconciliatorApiClient.class);

  /**
   * Constructor used by the Factory
   */
  protected ReconciliatorApiClient(final ReconciliatorApiFactory factory) {
    super(factory, factory.getUri());
  }

  @Override
  protected ReconciliationRequest getApiBusinessOutFromResponseForCreate(final Response response) {
    return null;
  }

  @Override
  protected Map<String, String> getHeadersFor(final ReconciliationRequest businessIn, final int context) {
    return Map.of();
  }

  public String createRequestCentral(final ReconciliationRequest request) throws CcsWithStatusException {
    final var uni = getService().createRequestCentral(request);
    try (final var response = exceptionMapper.handleUniResponse(uni)) {
      final var requestId = response.getHeaderString(X_REQUEST_ID);
      if (ParametersChecker.isEmpty(requestId)) {
        throw new CcsWithStatusException(request, Response.Status.NOT_FOUND.getStatusCode());
      }
      return requestId;
    } catch (final CcsClientGenericException | CcsServerGenericException | ClientWebApplicationException e) {
      throw ClientResponseExceptionMapper.getBusinessException(e);
    } catch (final RuntimeException e) {
      throw new CcsWithStatusException(request, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage(),
          e);
    }
  }

  public ReconciliationRequest getRequestStatus(final String idRequest) throws CcsWithStatusException {
    final var uni = getService().getRequestStatus(idRequest);
    try {
      return (ReconciliationRequest) exceptionMapper.handleUniObject(this, uni);
    } catch (final CcsClientGenericException | CcsServerGenericException | ClientWebApplicationException e) {
      throw ClientResponseExceptionMapper.getBusinessException(e);
    } catch (final RuntimeException e) {
      throw new CcsWithStatusException(idRequest, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage(),
          e);
    }
  }

  public void createRequestLocal(final ReconciliationRequest request) throws CcsWithStatusException {
    final var uni = getService().createRequestLocal(request);
    try (final var ignored = exceptionMapper.handleUniResponse(uni)) {
      // Nothing
      LOGGER.debugf("Request locally created: %s", request);
    } catch (final CcsClientGenericException | CcsServerGenericException | ClientWebApplicationException e) {
      throw ClientResponseExceptionMapper.getBusinessException(e);
    } catch (final RuntimeException e) {
      throw new CcsWithStatusException(request, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage(),
          e);
    }
  }

  public void endRequestLocal(final String idRequest, final String remoteId) throws CcsWithStatusException {
    final var uni = getService().endRequestLocal(idRequest, remoteId);
    try (final var ignored = exceptionMapper.handleUniResponse(uni)) {
      // Nothing
      LOGGER.debugf("Request locally ended: %s %s", idRequest, remoteId);
    } catch (final CcsClientGenericException | CcsServerGenericException | ClientWebApplicationException e) {
      throw ClientResponseExceptionMapper.getBusinessException(e);
    } catch (final RuntimeException e) {
      throw new CcsWithStatusException(idRequest, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage(),
          e);
    }
  }

  public Iterator<ReconciliationSitesListing> getSitesListing(final String idRequest) throws CcsWithStatusException {
    prepareInputStreamToReceive(AccessorProperties.isInternalCompression(), null);
    final var uni = getService().getSitesListing(AccessorProperties.isInternalCompression(), idRequest);
    final var inputStream = getInputStreamBusinessOutFromUni(true, uni).inputStream();
    return StreamIteratorUtils.getIteratorFromInputStream(inputStream, ReconciliationSitesListing.class);
  }

  public void endRequestCentral(final String idRequest) throws CcsWithStatusException {
    final var uni = getService().endRequestCentral(idRequest);
    try (final var ignored = exceptionMapper.handleUniResponse(uni)) {
      // Nothing
      LOGGER.debugf("Request central ended: %s", idRequest);
    } catch (final CcsClientGenericException | CcsServerGenericException | ClientWebApplicationException e) {
      throw ClientResponseExceptionMapper.getBusinessException(e);
    } catch (final RuntimeException e) {
      throw new CcsWithStatusException(idRequest, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage(),
          e);
    }
  }

  public Iterator<ReconciliationSitesAction> getActionsListing(final String idRequest, final String remoteId)
      throws CcsWithStatusException {
    prepareInputStreamToReceive(AccessorProperties.isInternalCompression(), null);
    final var uni = getService().getActionsListing(AccessorProperties.isInternalCompression(), idRequest, remoteId);
    final var inputStream = getInputStreamBusinessOutFromUni(true, uni).inputStream();
    return StreamIteratorUtils.getIteratorFromInputStream(inputStream, ReconciliationSitesAction.class);
  }

  public ReconciliationRequest getLocalRequestStatus(final String idRequest) throws CcsWithStatusException {
    final var uni = getService().getLocalRequestStatus(idRequest);
    try {
      return (ReconciliationRequest) exceptionMapper.handleUniObject(this, uni);
    } catch (final CcsClientGenericException | CcsServerGenericException | ClientWebApplicationException e) {
      throw ClientResponseExceptionMapper.getBusinessException(e);
    } catch (final RuntimeException e) {
      throw new CcsWithStatusException(idRequest, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage(),
          e);
    }
  }

  public String launchPurge(final String clientId, final long expiredInSeconds) throws CcsWithStatusException {
    final var uni = getService().launchPurge(clientId, expiredInSeconds);
    try (final var response = exceptionMapper.handleUniResponse(uni)) {
      final var requestId = response.getHeaderString(X_REQUEST_ID);
      if (ParametersChecker.isEmpty(requestId)) {
        throw new CcsWithStatusException(clientId, Response.Status.NOT_FOUND.getStatusCode());
      }
      return requestId;
    } catch (final CcsClientGenericException | CcsServerGenericException | ClientWebApplicationException e) {
      throw ClientResponseExceptionMapper.getBusinessException(e);
    } catch (final RuntimeException e) {
      throw new CcsWithStatusException(clientId, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage(),
          e);
    }
  }

  public Object getPurgeStatus(final String idPurge) throws CcsWithStatusException {
    final var uni = getService().getPurgeStatus(idPurge);
    try {
      return exceptionMapper.handleUniObject(this, uni);
    } catch (final CcsClientGenericException | CcsServerGenericException | ClientWebApplicationException e) {
      throw ClientResponseExceptionMapper.getBusinessException(e);
    } catch (final RuntimeException e) {
      throw new CcsWithStatusException(idPurge, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage(),
          e);
    }
  }

  public String launchImport(final String bucket, final String clientId, final long expiredInSeconds,
                             final String defaultMetadata) throws CcsWithStatusException {
    final var uni = getService().launchImport(bucket, clientId, expiredInSeconds, defaultMetadata);
    try (final var response = exceptionMapper.handleUniResponse(uni)) {
      final var requestId = response.getHeaderString(X_REQUEST_ID);
      if (ParametersChecker.isEmpty(requestId)) {
        throw new CcsWithStatusException(bucket, Response.Status.NOT_FOUND.getStatusCode());
      }
      return requestId;
    } catch (final CcsClientGenericException | CcsServerGenericException | ClientWebApplicationException e) {
      throw ClientResponseExceptionMapper.getBusinessException(e);
    } catch (final RuntimeException e) {
      throw new CcsWithStatusException(bucket, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage(),
          e);
    }
  }

  public Object getImportStatus(final String bucket, final String idImport) throws CcsWithStatusException {
    final var uni = getService().getImportStatus(bucket, idImport);
    try {
      return exceptionMapper.handleUniObject(this, uni);
    } catch (final CcsClientGenericException | CcsServerGenericException | ClientWebApplicationException e) {
      throw ClientResponseExceptionMapper.getBusinessException(e);
    } catch (final RuntimeException e) {
      throw new CcsWithStatusException(idImport, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage(),
          e);
    }
  }

  public String launchSync(final String bucket, final String clientId, final String targetSite)
      throws CcsWithStatusException {
    final var uni = getService().launchSync(bucket, clientId, targetSite);
    try (final var response = exceptionMapper.handleUniResponse(uni)) {
      final var requestId = response.getHeaderString(X_REQUEST_ID);
      if (ParametersChecker.isEmpty(requestId)) {
        throw new CcsWithStatusException(bucket, Response.Status.NOT_FOUND.getStatusCode());
      }
      return requestId;
    } catch (final CcsClientGenericException | CcsServerGenericException | ClientWebApplicationException e) {
      throw ClientResponseExceptionMapper.getBusinessException(e);
    } catch (final RuntimeException e) {
      throw new CcsWithStatusException(bucket, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage(),
          e);
    }
  }
}
