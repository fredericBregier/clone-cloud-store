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

package io.clonecloudstore.accessor.replicator.topic;

import java.util.List;

import io.clonecloudstore.accessor.replicator.application.RequestActionService;
import io.clonecloudstore.common.quarkus.client.SimpleClientAbstract;
import io.clonecloudstore.common.quarkus.metrics.BulkMetrics;
import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.replicator.model.ReplicatorOrder;
import io.smallrye.common.annotation.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import static io.clonecloudstore.replicator.config.ReplicatorConstants.Topic.REPLICATOR_ACTION_IN;

@ApplicationScoped
public class RequestActionConsumer {
  private static final Logger LOGGER = Logger.getLogger(RequestActionConsumer.class);
  public static final String TAG_CREATE = "create";
  public static final String TAG_DELETE = "delete";
  private final RequestActionService requestActionService;
  private final BulkMetrics bulkMetrics;

  public RequestActionConsumer(final RequestActionService requestActionService, final BulkMetrics bulkMetrics) {
    this.requestActionService = requestActionService;
    this.bulkMetrics = bulkMetrics;
  }

  @Incoming(REPLICATOR_ACTION_IN)
  @Blocking
  public void consumeOrder(final List<ReplicatorOrder> replicatorOrders) {
    QuarkusProperties.refreshModuleMdc();
    int delObject = 0;
    int delBucket = 0;
    int creObject = 0;
    int creBucket = 0;
    for (final var replicatorOrder : replicatorOrders) {
      try {
        SimpleClientAbstract.setMdcOpId(replicatorOrder.opId());
        LOGGER.debugf("Recv Order %s", replicatorOrder);
        switch (replicatorOrder.action()) {
          case DELETE -> {
            if (ParametersChecker.isNotEmpty(replicatorOrder.objectName())) {
              deleteObject(replicatorOrder);
              delObject++;
            } else {
              deleteBucket(replicatorOrder);
              delBucket++;
            }
          }
          case CREATE -> {
            if (ParametersChecker.isNotEmpty(replicatorOrder.objectName())) {
              createObject(replicatorOrder);
              creObject++;
            } else {
              createBucket(replicatorOrder);
              creBucket++;
            }
          }
          default -> LOGGER.warnf("Unknown order: %s", replicatorOrder);
        }
        LOGGER.debugf("Done Order %s", replicatorOrder);
      } catch (final RuntimeException e) {
        LOGGER.warn(replicatorOrder + " => " + e.getMessage());
      }
    }
    bulkMetricsUpdates(creBucket, creObject, delBucket, delObject);
  }

  private void bulkMetricsUpdates(final int creBucket, final int creObject, final int delBucket, final int delObject) {
    if (creBucket > 0) {
      bulkMetrics.getCounter(RequestActionConsumer.class, BulkMetrics.KEY_BUCKET, TAG_CREATE).increment(creBucket);
    }
    if (creObject > 0) {
      bulkMetrics.getCounter(RequestActionConsumer.class, BulkMetrics.KEY_OBJECT, TAG_CREATE).increment(creObject);
    }
    if (delBucket > 0) {
      bulkMetrics.getCounter(RequestActionConsumer.class, BulkMetrics.KEY_BUCKET, TAG_DELETE).increment(delBucket);
    }
    if (delObject > 0) {
      bulkMetrics.getCounter(RequestActionConsumer.class, BulkMetrics.KEY_OBJECT, TAG_DELETE).increment(delObject);
    }
  }

  void deleteBucket(final ReplicatorOrder order) {
    LOGGER.debugf("DeleteBucket %s", order);
    requestActionService.deleteBucket(order.bucketName());
  }

  void createBucket(final ReplicatorOrder order) {
    LOGGER.debugf("CreateBucket %s", order);
    requestActionService.createBucket(order.clientId(), order.bucketName());
  }

  void deleteObject(final ReplicatorOrder order) {
    LOGGER.debugf("DeleteObject %s", order);
    requestActionService.deleteObject(order.bucketName(), order.objectName());
  }

  void createObject(final ReplicatorOrder order) {
    LOGGER.debugf("CreateObject %s", order);
    requestActionService.createObject(order);
  }
}
