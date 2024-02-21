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
import io.quarkus.arc.Unremovable;
import io.smallrye.reactive.messaging.annotations.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import static io.clonecloudstore.replicator.config.ReplicatorConstants.Topic.REPLICATOR_ACTION_IN;

@ApplicationScoped
@Unremovable
public class RequestActionConsumer {
  private static final Logger LOGGER = Logger.getLogger(RequestActionConsumer.class);
  private final RequestActionService requestActionService;
  private final BulkMetrics bulkMetrics;

  public RequestActionConsumer(final RequestActionService requestActionService, final BulkMetrics bulkMetrics) {
    this.requestActionService = requestActionService;
    this.bulkMetrics = bulkMetrics;
  }

  @Incoming(REPLICATOR_ACTION_IN)
  @Blocking(ordered = true)
  public void consumeOrder(final List<ReplicatorOrder> replicatorOrders) {
    QuarkusProperties.refreshModuleMdc();
    int delObject = 0;
    int delBucket = 0;
    int creObject = 0;
    int creBucket = 0;
    int errorBucket = 0;
    int errorObject = 0;
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
        if (ParametersChecker.isNotEmpty(replicatorOrder.objectName())) {
          errorObject++;
        } else {
          errorBucket++;
        }
        LOGGER.warn(replicatorOrder + " => " + e.getMessage());
      }
    }
    bulkMetricsUpdates(creBucket, creObject, delBucket, delObject, errorBucket, errorObject);
  }

  private void bulkMetricsUpdates(final int creBucket, final int creObject, final int delBucket, final int delObject,
                                  final int errorBucket, final int errorObject) {
    if (creBucket > 0) {
      bulkMetrics.incrementCounter(creBucket, RequestActionConsumer.class, BulkMetrics.KEY_BUCKET,
          BulkMetrics.TAG_CREATE);
    }
    if (creObject > 0) {
      bulkMetrics.incrementCounter(creObject, RequestActionConsumer.class, BulkMetrics.KEY_OBJECT,
          BulkMetrics.TAG_CREATE);
    }
    if (delBucket > 0) {
      bulkMetrics.incrementCounter(delBucket, RequestActionConsumer.class, BulkMetrics.KEY_BUCKET,
          BulkMetrics.TAG_DELETE);
    }
    if (delObject > 0) {
      bulkMetrics.incrementCounter(delObject, RequestActionConsumer.class, BulkMetrics.KEY_OBJECT,
          BulkMetrics.TAG_DELETE);
    }
    if (errorBucket > 0) {
      bulkMetrics.incrementCounter(errorBucket, RequestActionConsumer.class, BulkMetrics.KEY_BUCKET,
          BulkMetrics.TAG_ERROR);
    }
    if (errorObject > 0) {
      bulkMetrics.incrementCounter(errorObject, RequestActionConsumer.class, BulkMetrics.KEY_OBJECT,
          BulkMetrics.TAG_ERROR);
    }
  }

  void deleteBucket(final ReplicatorOrder order) {
    LOGGER.debugf("DeleteBucket %s", order);
    requestActionService.deleteBucket(order);
  }

  void createBucket(final ReplicatorOrder order) {
    LOGGER.debugf("CreateBucket %s", order);
    requestActionService.createBucket(order);
  }

  void deleteObject(final ReplicatorOrder order) {
    LOGGER.debugf("DeleteObject %s", order);
    requestActionService.deleteObject(order);
  }

  void createObject(final ReplicatorOrder order) {
    LOGGER.debugf("CreateObject %s", order);
    requestActionService.createObject(order);
  }
}
