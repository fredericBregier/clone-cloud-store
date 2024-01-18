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

package io.clonecloudstore.accessor.client;

import java.util.Collection;

import io.clonecloudstore.accessor.client.api.AccessorBucketApi;
import io.clonecloudstore.accessor.client.model.AccessorHeaderDtoConverter;
import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.common.quarkus.client.SimpleClientAbstract;
import io.clonecloudstore.common.quarkus.client.utils.ClientResponseExceptionMapper;
import io.clonecloudstore.common.quarkus.exception.CcsClientGenericException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericException;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.driver.api.StorageType;
import jakarta.ws.rs.core.Response;
import org.jboss.resteasy.reactive.ClientWebApplicationException;

/**
 * Client for Accessor Bucket
 */
public class AccessorBucketApiClient extends SimpleClientAbstract<AccessorBucketApi> {
  /**
   * Constructor used by the Factory
   */
  protected AccessorBucketApiClient(final AccessorBucketApiFactory factory) {
    super(factory, factory.getUri());
  }

  /**
   * @return the collection of Buckets
   */
  public Collection<AccessorBucket> getBuckets(final String clientId) throws CcsWithStatusException {
    final var uni = getService().getBuckets(clientId, getOpId());
    try {
      return (Collection<AccessorBucket>) exceptionMapper.handleUniObject(this, uni);
    } catch (final CcsClientGenericException | CcsServerGenericException | ClientWebApplicationException e) {
      throw ClientResponseExceptionMapper.getBusinessException(e);
    } catch (final RuntimeException e) {
      throw new CcsWithStatusException(null, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage(), e);
    }
  }

  /**
   * @return the StorageType for this Bucket
   */
  public StorageType checkBucket(final String bucketName, final String clientId) throws CcsWithStatusException {
    final var uni = getService().checkBucket(bucketName, clientId, getOpId());
    try (final var response = exceptionMapper.handleUniResponse(uni)) {
      return AccessorHeaderDtoConverter.getStorageTypeFromResponse(response);
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
        return StorageType.NONE;
      }
      throw e;
    } catch (final CcsClientGenericException | CcsServerGenericException | ClientWebApplicationException e) {
      throw ClientResponseExceptionMapper.getBusinessException(e);
    } catch (final RuntimeException e) {
      throw new CcsWithStatusException(bucketName, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          e.getMessage(), e);
    }
  }

  /**
   * @return the Bucket Metadata
   */
  public AccessorBucket getBucket(final String bucketName, final String clientId) throws CcsWithStatusException {
    final var uni = getService().getBucket(bucketName, clientId, getOpId());
    return (AccessorBucket) exceptionMapper.handleUniObject(this, uni);
  }

  /**
   * @return the DTO if the Bucket is created
   */
  public AccessorBucket createBucket(final String bucketName, final String clientId) throws CcsWithStatusException {
    final var uni = getService().createBucket(bucketName, clientId, getOpId());
    return (AccessorBucket) exceptionMapper.handleUniObject(this, uni);
  }

  /**
   * @return True if deleted (or already deleted), else False or an exception
   */
  public boolean deleteBucket(final String bucketName, final String clientId) throws CcsWithStatusException {
    final var uni = getService().deleteBucket(bucketName, clientId, getOpId());
    try (final var response = exceptionMapper.handleUniResponse(uni)) {
      return response.getStatus() == Response.Status.NO_CONTENT.getStatusCode();
    } catch (final CcsWithStatusException e) {
      if (e.getStatus() == Response.Status.GONE.getStatusCode()) {
        return true;
      }
      throw e;
    } catch (final CcsClientGenericException | CcsServerGenericException | ClientWebApplicationException e) {
      throw ClientResponseExceptionMapper.getBusinessException(e);
    } catch (final RuntimeException e) {
      throw new CcsWithStatusException(bucketName, Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          e.getMessage(), e);
    }
  }

}
