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

package io.clonecloudstore.replicator.server.local.application;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

import io.clonecloudstore.accessor.model.AccessorBucket;
import io.clonecloudstore.administration.client.TopologyApiClientFactory;
import io.clonecloudstore.administration.model.Topology;
import io.clonecloudstore.administration.model.TopologyStatus;
import io.clonecloudstore.common.quarkus.modules.ServiceProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import io.clonecloudstore.driver.api.StorageType;
import io.clonecloudstore.replicator.server.remote.client.RemoteReplicatorApiClient;
import io.clonecloudstore.replicator.server.remote.client.RemoteReplicatorApiClientFactory;
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

@ApplicationScoped
@Unremovable
public class LocalReplicatorService {
  private static final Logger LOGGER = Logger.getLogger(LocalReplicatorService.class);
  private final TopologyApiClientFactory topologyApiClientFactory;
  private final RemoteReplicatorApiClientFactory remoteReplicatorApiClientFactory;

  public LocalReplicatorService(final TopologyApiClientFactory topologyApiClientFactory,
                                final RemoteReplicatorApiClientFactory remoteReplicatorApiClientFactory) {
    this.topologyApiClientFactory = topologyApiClientFactory;
    this.remoteReplicatorApiClientFactory = remoteReplicatorApiClientFactory;
  }

  public Collection<Topology> getTopologies(final String topologyId) throws CcsWithStatusException {
    final Collection<Topology> topologies;
    try (final var topologyClient = topologyApiClientFactory.newClient()) {
      if (ParametersChecker.isNotEmpty(topologyId)) {
        topologies = new ArrayList<>();
        final var topology = topologyClient.findBySite(topologyId);
        if (topology != null) {
          topologies.add(topology);
        }
      } else {
        topologies = topologyClient.listWithStatus(TopologyStatus.UP);
      }
    }
    return topologies;
  }

  public Optional<Topology> findValidTopologyBucket(final String bucket, final boolean fullCheck, final String clientId,
                                                    final String topologyId, final String opId)
      throws CcsWithStatusException {
    return findValidTopology(topologyId, bucket, null, fullCheck, clientId, opId);
  }

  public AccessorBucket getBucket(final String bucket, final String clientId, final Topology topology,
                                  final String opId) throws CcsWithStatusException {
    final var remoteReplicatorUri = URI.create(topology.uri());
    try (final RemoteReplicatorApiClient client = remoteReplicatorApiClientFactory.newClient(remoteReplicatorUri)) {
      return client.getBucket(bucket, clientId, opId);
    }
  }

  public Optional<Topology> findValidTopologyObject(final String bucket, final String objectName,
                                                    final boolean fullCheck, final String clientId,
                                                    final String topologyId, final String opId)
      throws CcsWithStatusException {
    return findValidTopology(topologyId, bucket, objectName, fullCheck, clientId, opId);
  }

  private Optional<Topology> checkOneTopology(final Topology topology, final String bucket, final String objectName,
                                              final boolean fullCheck, final String clientId, final String opId)
      throws CcsWithStatusException {
    final var remoteReplicatorUri = URI.create(topology.uri());
    try (final RemoteReplicatorApiClient client = remoteReplicatorApiClientFactory.newClient(remoteReplicatorUri)) {
      if (ParametersChecker.isNotEmpty(objectName)) {
        LOGGER.debugf("Check object with remote replicator : %s", topology.uri());
        final var storageType = client.checkObjectOrDirectoryCache(bucket, objectName, fullCheck, clientId, opId);
        if (StorageType.OBJECT.equals(storageType)) {
          // Store Topology Candidate
          return Optional.of(topology);
        }
      } else {
        LOGGER.debugf("Check bucket with remote replicator : %s", topology.uri());
        final var storageType = client.checkBucketCache(bucket, fullCheck, clientId, opId);
        if (StorageType.BUCKET.equals(storageType)) {
          // Store Topology Candidate
          return Optional.of(topology);
        }
      }
    }
    throw new CcsWithStatusException(topology, 404);
  }

  private Optional<Topology> findValidTopology(final String topologyId, final String bucket, final String objectName,
                                               final boolean fullCheck, final String clientId, final String opId)
      throws CcsWithStatusException {
    final Collection<Topology> topologies = getTopologies(topologyId);
    for (final var topology : topologies) {
      // ignores local replicator by checking configuration
      if (!ServiceProperties.getAccessorSite().equals(topology.id())) {
        try {
          return checkOneTopology(topology, bucket, objectName, fullCheck, clientId, opId);
        } catch (final CcsWithStatusException e) {
          if (e.getStatus() != 404) {
            throw e;
          }
        }
      }
    }
    return Optional.empty();
  }

  /*
  FIXME When StructuredTaskScope in standard Java, not experimental

  private Optional<Topology> findValidTopologyJdk22(final String topologyId, final String bucket,
                                                    final String objectName, final boolean fullCheck,
                                                    final String clientId, final String opId)
      throws CcsWithStatusException {
    final Collection<Topology> topologies = getTopologies(topologyId);
    try (var scope = new StructuredTaskScope.ShutdownOnSuccess<Optional<Topology>>()) {
      for (final var topology : topologies) {
        // ignores local replicator by checking configuration
        if (!ReplicatorProperties.getAccessorSite().equals(topology.id())) {
          scope.fork(() -> {
            return checkOneTopology(topology, bucket, objectName, fullCheck, clientId, opId);
          });
        }
      }
      scope.join();
      return scope.result();
    } catch (final InterruptedException | ExecutionException e) { // NOSONAR intentional
      LOGGER.error(e, e);
      return Optional.empty();
    }
  }
   */
}
