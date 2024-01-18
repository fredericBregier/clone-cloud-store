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

import io.clonecloudstore.administration.client.api.OwnershipApi;
import io.clonecloudstore.administration.database.model.DaoOwnershipRepository;
import io.clonecloudstore.administration.model.ClientBucketAccess;
import io.clonecloudstore.administration.model.ClientOwnership;
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
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.COLL_OWNERSHIPS;


@Path(ADMINISTRATION_ROOT + COLL_OWNERSHIPS)
public class OwnershipResource implements OwnershipApi {
  private static final Logger LOGGER = Logger.getLogger(OwnershipResource.class);
  private final DaoOwnershipRepository repository;

  public OwnershipResource(final Instance<DaoOwnershipRepository> repositoryInstance) {
    this.repository = repositoryInstance.get();
  }

  @Override
  public Uni<Collection<ClientBucketAccess>> listAll(@RestPath final String client,
                                                     @QueryParam("ownership") @DefaultValue("UNKNOWN") final ClientOwnership ownership) {
    return Uni.createFrom().emitter(em -> {
      try {
        if (ClientOwnership.UNKNOWN.equals(ownership)) {
          em.complete(repository.findAllOwnerships(client));
        } else {
          em.complete(repository.findOwnerships(client, ownership));
        }
      } catch (final CcsDbException e) {
        LOGGER.errorf("Could not find all Ownership: %s", e.getMessage());
        em.fail(new CcsOperationException(e.getMessage()));
      } catch (final RuntimeException e) {
        ServerResponseFilter.handleExceptionFail(em, e);
      }
    });
  }

  @Override
  public Uni<ClientOwnership> findByBucket(final String client, final String bucket) {
    return Uni.createFrom().emitter(em -> {
      try {
        final var ownership = repository.findByBucket(client, bucket);
        if (ownership != null) {
          em.complete(ownership);
        } else {
          em.fail(new CcsNotExistException(Response.Status.NOT_FOUND.getReasonPhrase()));
        }
      } catch (final CcsDbException e) {
        LOGGER.errorf("Could not find Ownership: %s", e.getMessage());
        em.fail(new CcsNotExistException(e.getMessage()));
      } catch (final RuntimeException e) {
        ServerResponseFilter.handleExceptionFail(em, e);
      }
    });
  }

  @Override
  public Uni<ClientOwnership> add(final String client, final String bucket, final ClientOwnership ownership) {
    return Uni.createFrom().emitter(em -> {
      try {
        em.complete(repository.insertOwnership(client, bucket, ownership));
      } catch (final CcsDbException e) {
        LOGGER.errorf("Could not add Ownership: %s", e.getMessage());
        em.fail(new CcsAlreadyExistException(e.getMessage()));
      } catch (final RuntimeException e) {
        ServerResponseFilter.handleExceptionFail(em, e);
      }
    });
  }

  @Override
  public Uni<ClientOwnership> update(final String client, final String bucket, final ClientOwnership ownership) {
    return Uni.createFrom().emitter(em -> {
      try {
        em.complete(repository.updateOwnership(client, bucket, ownership));
      } catch (final CcsDbException e) {
        LOGGER.errorf("Could not update Ownership: %s", e.getMessage());
        em.fail(new CcsNotExistException(e.getMessage()));
      } catch (final RuntimeException e) {
        ServerResponseFilter.handleExceptionFail(em, e);
      }
    });
  }

  @Override
  public Uni<Response> delete(final String client, final String bucket) {
    return Uni.createFrom().emitter(em -> {
      try {
        repository.deleteOwnership(client, bucket);
        em.complete(Response.noContent().build());
      } catch (final CcsDbException e) {
        LOGGER.errorf("Could not delete Ownership: %s", e.getMessage());
        em.fail(new CcsNotExistException(e.getMessage()));
      } catch (final RuntimeException e) {
        ServerResponseFilter.handleException(em, e);
      }
    });
  }

  @Override
  public Uni<Response> deleteAllClient(final String bucket) {
    return Uni.createFrom().emitter(em -> {
      try {
        repository.deleteOwnerships(bucket);
        em.complete(Response.noContent().build());
      } catch (final CcsDbException e) {
        LOGGER.errorf("Could not delete Ownership: %s", e.getMessage());
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
