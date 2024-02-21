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

package io.clonecloudstore.replicator.server.local.topic;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import io.clonecloudstore.administration.model.Topology;
import io.clonecloudstore.common.quarkus.metrics.BulkMetrics;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;
import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.replicator.config.ReplicatorConstants;
import io.clonecloudstore.replicator.model.ReplicatorOrder;
import io.clonecloudstore.replicator.server.local.application.LocalReplicatorService;
import io.clonecloudstore.replicator.server.remote.client.RemoteReplicatorApiClientFactory;
import io.quarkus.arc.Unremovable;
import io.smallrye.reactive.messaging.annotations.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import static io.clonecloudstore.common.quarkus.metrics.BulkMetrics.TAG_REPLICATE;

@ApplicationScoped
@Unremovable
public class LocalReplicatorRequestTopicConsumer {
  private static final Logger LOGGER = Logger.getLogger(LocalReplicatorRequestTopicConsumer.class);
  private final RemoteReplicatorApiClientFactory remoteReplicatorApiClientFactory;
  private final LocalReplicatorService replicatorService;
  private final BulkMetrics bulkMetrics;

  public LocalReplicatorRequestTopicConsumer(final RemoteReplicatorApiClientFactory remoteReplicatorApiClientFactory,
                                             final LocalReplicatorService replicatorService,
                                             final BulkMetrics bulkMetrics) {
    this.remoteReplicatorApiClientFactory = remoteReplicatorApiClientFactory;
    this.replicatorService = replicatorService;
    this.bulkMetrics = bulkMetrics;
  }

  @Incoming(ReplicatorConstants.Topic.REPLICATOR_REQUEST_IN)
  @Blocking(ordered = true)
  public void consumeOrder(final List<ReplicatorOrder> replicatorOrders) {
    QuarkusProperties.refreshModuleMdc();
    final Map<String, Topology> mapTopologies;
    try {
      final var topologiesTemp = replicatorService.getTopologies(null).stream().distinct().toList();
      final var optional =
          topologiesTemp.stream().filter(topology -> Objects.equals(topology.id(), ServiceProperties.getAccessorSite()))
              .findFirst();
      mapTopologies = topologiesTemp.stream().collect(Collectors.toMap(Topology::id, topology -> topology));
      optional.ifPresent(topology -> mapTopologies.remove(topology.id()));
    } catch (final CcsWithStatusException e) {
      LOGGER.error(e.getMessage());
      // Element in error will be ignored, letting the reconciliation doing the job
      return;
    }
    final var mapItemsPerTarget = new HashMap<String, List<ReplicatorOrder>>();
    try {
      // Element in error will be ignored, letting the reconciliation doing the job
      // TODO However could optimize reconciliation by storing the ReplicatorOrders in error to redo them
      prepareOrders(replicatorOrders, mapItemsPerTarget, mapTopologies);
      for (final var set : mapItemsPerTarget.entrySet()) {
        final var list = set.getValue();
        if (list != null && !list.isEmpty()) {
          final var topology = mapTopologies.get(set.getKey());
          createOrderWithClientAssociatedWithTopology(topology, list);
          list.clear();
        }
      }
    } finally {
      mapItemsPerTarget.clear();
      mapTopologies.clear();
    }
  }

  private void createOrderWithClientAssociatedWithTopology(final Topology topology, final List<ReplicatorOrder> list) {
    try (final var client = remoteReplicatorApiClientFactory.newClient(URI.create(topology.uri()))) {
      try {
        if (list.size() == 1) {
          client.createOrder(list.getFirst()).await().atMost(QuarkusProperties.getDurationResponseTimeout()).close();
        } else {
          client.createOrders(list).await().atMost(QuarkusProperties.getDurationResponseTimeout()).close();
        }
        bulkMetrics.incrementCounter(list.size(), LocalReplicatorRequestTopicConsumer.class, BulkMetrics.KEY_ORDER,
            TAG_REPLICATE);
      } catch (final RuntimeException e) {
        // Ignored
        LOGGER.debugf("Ignored: %s", e.getMessage());
      }
    }
  }

  private void prepareSend(final ReplicatorOrder replicatorOrder,
                           final Map<String, List<ReplicatorOrder>> mapItemsPerTarget) {
    final var list = mapItemsPerTarget.computeIfAbsent(replicatorOrder.toSite(), k -> new ArrayList<>());
    list.add(replicatorOrder);
  }

  private void prepareOrders(final List<ReplicatorOrder> replicatorOrders,
                             final HashMap<String, List<ReplicatorOrder>> mapItemsPerTarget,
                             final Map<String, Topology> mapTopologies) {
    for (final var replicatorOrder : replicatorOrders) {
      LOGGER.debugf("Will Send to all %s", replicatorOrder);
      final var toSite = replicatorOrder.toSite();
      if (ParametersChecker.isNotEmpty(toSite)) {
        prepareSend(replicatorOrder, mapItemsPerTarget);
      } else {
        for (final var topology : mapTopologies.keySet()) {
          final var clone = new ReplicatorOrder(replicatorOrder, topology);
          prepareSend(clone, mapItemsPerTarget);
        }
      }
    }
  }
}
