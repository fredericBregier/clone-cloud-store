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

package io.clonecloudstore.accessor.server.application;

import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.common.quarkus.client.InputStreamBusinessOut;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.exception.CcsServerGenericExceptionMapper;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.replicator.client.LocalReplicatorApiClientFactory;
import io.clonecloudstore.replicator.model.ReplicatorOrder;
import io.clonecloudstore.replicator.model.ReplicatorResponse;
import io.clonecloudstore.replicator.topic.LocalBrokerService;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Interface of Replicator Client and Broker
 */
@ApplicationScoped
public class LocalReplicatorService {
  private final LocalBrokerService localBrokerService;
  private final LocalReplicatorApiClientFactory localReplicatorApiClientFactory;
  private final LocalReplicatorOrderEmitter localReplicatorOrderEmitter;

  public LocalReplicatorService(final LocalBrokerService localBrokerService,
                                final LocalReplicatorApiClientFactory localReplicatorApiClientFactory,
                                final LocalReplicatorOrderEmitter localReplicatorOrderEmitter) {
    this.localBrokerService = localBrokerService;
    this.localReplicatorApiClientFactory = localReplicatorApiClientFactory;
    this.localReplicatorOrderEmitter = localReplicatorOrderEmitter;
  }

  /**
   * Through Broker, asks to replicate this Bucket
   */
  public void create(final String bucketName, final String clientId) throws CcsOperationException {
    try {
      //Send use replicator service to send create order.
      localBrokerService.createBucket(bucketName, clientId);
    } catch (final RuntimeException e) {
      throw new CcsOperationException("Relicator Bucket Creation error", e);
    }
  }

  /**
   * Through Broker, asks to delete this Bucket
   */
  public void delete(final String bucketName, final String clientId) throws CcsOperationException {
    try {
      //Send use replicator service to send delete order.
      localBrokerService.deleteBucket(bucketName, clientId);
    } catch (final RuntimeException e) {
      throw new CcsOperationException("Relicator Bucket Deletion error", e);
    }
  }

  /**
   * Through Broker, asks to delete this Object
   */
  public void delete(final String bucketName, final String objectName, final String clientId)
      throws CcsOperationException {
    try {
      //Send use replicator service to send delete order.
      localBrokerService.deleteObject(bucketName, objectName, clientId);
    } catch (final RuntimeException e) {
      throw new CcsOperationException("Relicator Object Deletion error", e);
    }
  }

  /**
   * Through Broker, asks to replicate this Object
   */
  public void create(final String bucketName, final String objectName, final String clientId, final long size,
                     final String hash) throws CcsOperationException {
    try {
      //Send use replicator service to send create order.
      localBrokerService.createObject(bucketName, objectName, clientId, size, hash);
    } catch (final RuntimeException e) {
      throw new CcsOperationException("Relicator Object Creation error", e);
    }
  }

  /**
   * Through API Client, asks if this Bucket exists in remote
   */
  public boolean remoteCheckBucket(final String bucketName, final String clientId, final String opId)
      throws CcsOperationException {
    try (final var client = localReplicatorApiClientFactory.newClient()) {
      client.setOpId(opId);
      return StorageType.BUCKET.equals(client.checkBucket(bucketName, false, clientId, opId).response());
    } catch (final CcsWithStatusException e) {
      throw CcsServerGenericExceptionMapper.getCcsException(e.getStatus(), "Relicator Bucket Check error", e);
    }
  }

  /**
   * Through API Client, asks if this Bucket exists in remote
   */
  public ReplicatorResponse<AccessorBucket> remoteGetBucket(final String bucketName, final String clientId,
                                                            final String opId) throws CcsOperationException {
    try (final var client = localReplicatorApiClientFactory.newClient()) {
      client.setOpId(opId);
      return client.getBucket(bucketName, clientId, opId);
    } catch (final CcsWithStatusException e) {
      throw CcsServerGenericExceptionMapper.getCcsException(e.getStatus(), "Relicator Bucket Check error", e);
    }
  }

  /**
   * Through API Client, asks if this Object exists in remote
   */
  public ReplicatorResponse<StorageType> remoteCheckObject(final String bucketName, final String objectName,
                                                           final String clientId, final String opId)
      throws CcsOperationException {
    try (final var client = localReplicatorApiClientFactory.newClient()) {
      client.setOpId(opId);
      return client.checkObjectOrDirectory(bucketName, objectName, false, clientId, opId);
    } catch (final CcsWithStatusException e) {
      throw CcsServerGenericExceptionMapper.getCcsException(e.getStatus(), "Relicator Object Check error", e);
    }
  }

  /**
   * Through API Client, Get this Object and Content from remote
   */
  public InputStreamBusinessOut<AccessorObject> remoteReadObject(final String bucketName, final String objectName,
                                                                 final String clientId, final String targetId,
                                                                 final String opId) throws CcsOperationException {
    try (final var client = localReplicatorApiClientFactory.newClient()) {
      client.setOpId(opId);
      return client.readRemoteObject(bucketName, objectName, clientId, targetId, opId);
    } catch (final CcsWithStatusException e) {
      throw CcsServerGenericExceptionMapper.getCcsException(e.getStatus(), "Relicator Object Read error", e);
    }
  }

  /**
   * Local Replication order as self repairing
   */
  public void generateLocalReplicationOrder(final ReplicatorOrder replicatorOrder) {
    localReplicatorOrderEmitter.generate(replicatorOrder);
  }
}
