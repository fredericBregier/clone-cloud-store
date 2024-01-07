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

package io.clonecloudstore.replicator.topic;

import io.clonecloudstore.common.quarkus.client.SimpleClientAbstract;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;
import io.clonecloudstore.replicator.config.ReplicatorConstants.Action;
import io.clonecloudstore.replicator.model.ReplicatorOrder;
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.jboss.logging.Logger;

import static io.clonecloudstore.replicator.config.ReplicatorConstants.Action.CREATE;
import static io.clonecloudstore.replicator.config.ReplicatorConstants.Action.DELETE;
import static io.clonecloudstore.replicator.config.ReplicatorConstants.Topic.REPLICATOR_REQUEST_OUT;

@ApplicationScoped
@Unremovable
public class LocalBrokerService {
  private static final Logger logger = Logger.getLogger(LocalBrokerService.class);

  private final Emitter<ReplicatorOrder> replicatorOrderEmitter;

  public LocalBrokerService(@Channel(REPLICATOR_REQUEST_OUT) Emitter<ReplicatorOrder> replicatorOrderEmitter) {
    this.replicatorOrderEmitter = replicatorOrderEmitter;
  }

  public void createBucket(final String bucket, final String clientId) {
    logger.debugf("Send create bucket event to broadcast bucket replication topic : %s", bucket);
    broadcastBucket(bucket, clientId, CREATE);
  }

  public void deleteBucket(final String bucket, final String clientId) {
    logger.debugf("Send delete bucket event to broadcast bucket replication topic : %s", bucket);
    broadcastBucket(bucket, clientId, DELETE);
  }

  public void createObject(final String bucket, final String object, final String clientId, final long size,
                           final String hash) {
    logger.debugf("Send create object event to broadcast object replication topic :%s / %s", bucket, object);
    broadcastObject(bucket, object, clientId, size, hash, CREATE);
  }

  public void deleteObject(final String bucket, final String object, final String clientId) {
    logger.debugf("Send delete object event to broadcast object replication topic ::%s / %s", bucket, object);
    broadcastObject(bucket, object, clientId, 0, null, DELETE);
  }

  private void broadcastObject(final String bucket, final String object, final String clientId, final long size,
                               final String hash, final Action action) {
    try {
      final var replicatorOrder =
          new ReplicatorOrder(SimpleClientAbstract.getMdcOpId(), ServiceProperties.getAccessorSite(), null, clientId,
              bucket, object, size, hash, action);
      logger.debugf("Produce object replication message to broadcast object replication topic : %s", replicatorOrder);
      replicatorOrderEmitter.send(replicatorOrder);
    } catch (final RuntimeException e) {
      throw new CcsOperationException("Could not send replication broadcast event", e);
    }
  }

  private void broadcastBucket(final String bucket, final String clientId, final Action action) {
    try {
      final var replicatorOrder =
          new ReplicatorOrder(SimpleClientAbstract.getMdcOpId(), ServiceProperties.getAccessorSite(), null, clientId,
              bucket, action);
      logger.debugf("Produce bucket replication message to broadcast bucket replication topic : %s", replicatorOrder);
      replicatorOrderEmitter.send(replicatorOrder);
    } catch (final RuntimeException e) {
      throw new CcsOperationException("Could not send replication broadcast event", e);
    }
  }
}
