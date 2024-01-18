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

package io.clonecloudstore.administration.resource;

import java.util.Collection;

import io.clonecloudstore.administration.client.api.TopologyApi;
import io.clonecloudstore.administration.database.model.DaoTopologyRepository;
import io.clonecloudstore.administration.model.Topology;
import io.clonecloudstore.administration.model.TopologyStatus;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.exception.CcsAlreadyExistException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.common.quarkus.server.service.ServerResponseFilter;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.inject.Instance;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.RestPath;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.ADMINISTRATION_ROOT;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.COLL_TOPOLOGIES;


@Path(ADMINISTRATION_ROOT + COLL_TOPOLOGIES)
public class TopologyResource implements TopologyApi {
  private static final Logger LOGGER = Logger.getLogger(TopologyResource.class);
  private final DaoTopologyRepository repository;

  public TopologyResource(final Instance<DaoTopologyRepository> repositoryInstance) {
    this.repository = repositoryInstance.get();
  }

  @Override
  public Uni<Collection<Topology>> listAll(@QueryParam("status") @DefaultValue("UNKNOWN") final TopologyStatus status) {
    return Uni.createFrom().emitter(em -> {
      try {
        if (TopologyStatus.UNKNOWN.equals(status)) {
          em.complete(repository.findAllTopologies());
        } else {
          em.complete(repository.findTopologies(status));
        }
      } catch (final CcsDbException e) {
        LOGGER.errorf("Could not find all topologies: %s", e.getMessage());
        em.fail(new CcsOperationException(e.getMessage()));
      } catch (final RuntimeException e) {
        ServerResponseFilter.handleExceptionFail(em, e);
      }
    });
  }

  @Override
  public Uni<Topology> findBySite(@RestPath final String site) {
    return Uni.createFrom().emitter(em -> {
      try {
        final var topology = repository.findBySite(site);
        if (topology != null) {
          em.complete(topology);
        } else {
          em.fail(new CcsNotExistException(Response.Status.NOT_FOUND.getReasonPhrase()));
        }
      } catch (final CcsDbException e) {
        LOGGER.errorf("Could not find topology by site: %s", e.getMessage());
        em.fail(new CcsNotExistException(e.getMessage()));
      } catch (final RuntimeException e) {
        ServerResponseFilter.handleExceptionFail(em, e);
      }
    });
  }

  @Override
  public Uni<Topology> add(final Topology topology) {
    return Uni.createFrom().emitter(em -> {
      try {
        em.complete(repository.insertTopology(topology));
      } catch (final CcsDbException e) {
        LOGGER.errorf("Could not add topology: %s", e.getMessage());
        em.fail(new CcsAlreadyExistException(e.getMessage()));
      } catch (final RuntimeException e) {
        ServerResponseFilter.handleExceptionFail(em, e);
      }
    });
  }

  @Override
  public Uni<Topology> update(final Topology topology) {
    return Uni.createFrom().emitter(em -> {
      try {
        em.complete(repository.updateTopology(topology));
      } catch (final CcsDbException e) {
        LOGGER.errorf("Could not update topology: %s", e.getMessage());
        em.fail(new CcsNotExistException(e.getMessage()));
      } catch (final RuntimeException e) {
        ServerResponseFilter.handleExceptionFail(em, e);
      }
    });
  }

  @Override
  public Uni<Response> delete(@RestPath final String site) {
    return Uni.createFrom().emitter(em -> {
      try {
        repository.deleteTopology(site);
        em.complete(Response.noContent().build());
      } catch (final CcsDbException e) {
        LOGGER.errorf("Could not delete topology: %s", e.getMessage());
        em.fail(new CcsNotExistException(e.getMessage()));
      } catch (final RuntimeException e) {
        ServerResponseFilter.handleException(em, e);
      }
    });
  }

  @Override
  public void close() {
    // Empty
  }
}
