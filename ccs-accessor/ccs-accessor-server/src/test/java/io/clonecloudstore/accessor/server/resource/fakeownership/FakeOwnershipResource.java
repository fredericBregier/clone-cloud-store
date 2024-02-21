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

package io.clonecloudstore.accessor.server.resource.fakeownership;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.clonecloudstore.administration.client.api.OwnershipApi;
import io.clonecloudstore.administration.model.ClientBucketAccess;
import io.clonecloudstore.administration.model.ClientOwnership;
import io.clonecloudstore.common.quarkus.exception.CcsAlreadyExistException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsServerExceptionMapper;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.ADMINISTRATION_ROOT;
import static io.clonecloudstore.accessor.config.AccessorConstants.Api.COLL_OWNERSHIPS;


@Path(ADMINISTRATION_ROOT + COLL_OWNERSHIPS)
public class FakeOwnershipResource implements OwnershipApi {
  private static final Logger LOGGER = Logger.getLogger(FakeOwnershipResource.class);
  private final Map<String, Map<String, ClientBucketAccess>> repository = new HashMap<>();
  public static int errorCode = 0;

  @Override
  public Uni<Collection<ClientBucketAccess>> listAll(final String client, final ClientOwnership ownership) {
    return Uni.createFrom().emitter(em -> {
      if (errorCode > 0) {
        throw CcsServerExceptionMapper.getCcsException(errorCode, null, null);
      }
      if (ClientOwnership.UNKNOWN.equals(ownership)) {
        final var map = repository.get(client);
        if (map != null) {
          em.complete(map.values());
        } else {
          em.complete(Collections.emptyList());
        }
      } else {
        final var map = repository.get(client);
        if (map != null) {
          final var list = map.values();
          if (list.isEmpty()) {
            em.complete(list);
          } else {
            em.complete(list.stream().filter(topology -> topology.include(ownership)).toList());
          }
        } else {
          em.complete(Collections.emptyList());
        }
      }
    });
  }

  @Override
  public Uni<ClientOwnership> add(final String client, final String bucket, final ClientOwnership ownership) {
    return Uni.createFrom().emitter(em -> {
      final var map = repository.get(client);
      if (map != null) {
        if (map.containsKey(bucket)) {
          em.fail(new CcsAlreadyExistException(Response.Status.CONFLICT.getReasonPhrase()));
        } else {
          final var rcb = new ClientBucketAccess(client, bucket, ownership);
          map.put(bucket, rcb);
          em.complete(rcb.ownership());
          LOGGER.infof("Trace Add %s", rcb);
        }
      } else {
        final var newmap = new HashMap<String, ClientBucketAccess>();
        final var rcb = new ClientBucketAccess(client, bucket, ownership);
        newmap.put(bucket, rcb);
        repository.put(client, newmap);
        em.complete(rcb.ownership());
        LOGGER.infof("Trace Add %s", rcb);
      }
    });
  }

  @Override
  public Uni<ClientOwnership> update(final String client, final String bucket, final ClientOwnership ownership) {
    return Uni.createFrom().emitter(em -> {
      final var map = repository.get(client);
      if (map != null) {
        if (!map.containsKey(bucket)) {
          em.fail(new CcsNotExistException(Response.Status.NOT_FOUND.getReasonPhrase()));
        } else {
          final var rcbSource = map.get(bucket);
          final var rcb = new ClientBucketAccess(client, bucket, rcbSource.ownership().fusion(ownership));
          map.put(bucket, rcb);
          em.complete(rcb.ownership());
          LOGGER.infof("Trace Update %s", rcb);
        }
      } else {
        em.fail(new CcsNotExistException(Response.Status.NOT_FOUND.getReasonPhrase()));
      }
    });
  }

  @Override
  public Uni<ClientOwnership> findByBucket(final String client, final String bucket) {
    return Uni.createFrom().emitter(em -> {
      final var map = repository.get(client);
      if (map != null) {
        final var rcb = map.get(bucket);
        if (rcb != null) {
          em.complete(rcb.ownership());
          LOGGER.infof("Trace Find %s", rcb);
        } else {
          em.fail(new CcsNotExistException(Response.Status.NOT_FOUND.getReasonPhrase()));
        }
      } else {
        em.fail(new CcsNotExistException(Response.Status.NOT_FOUND.getReasonPhrase()));
      }
    });
  }

  @Override
  public Uni<Response> delete(final String client, final String bucket) {
    return Uni.createFrom().emitter(em -> {
      final var map = repository.get(client);
      if (map != null) {
        if (!map.containsKey(bucket)) {
          em.fail(new CcsNotExistException(Response.Status.NOT_FOUND.getReasonPhrase()));
        } else {
          final var rcb = map.remove(bucket);
          em.complete(Response.noContent().build());
          LOGGER.infof("Trace Delete %s", rcb);
        }
      } else {
        em.fail(new CcsNotExistException(Response.Status.NOT_FOUND.getReasonPhrase()));
      }
    });
  }

  @Override
  public Uni<Response> deleteAllClient(final String bucket) {
    return Uni.createFrom().emitter(em -> {
      boolean found = false;
      for (final var map : repository.values()) {
        if (map != null) {
          if (map.containsKey(bucket)) {
            final var rcb = map.remove(bucket);
            found = true;
            LOGGER.infof("Trace Delete %s", rcb);
          }
        }
      }
      if (found) {
        em.complete(Response.noContent().build());
      } else {
        em.fail(new CcsNotExistException(Response.Status.NOT_FOUND.getReasonPhrase()));
      }
    });
  }

  @Override
  public void close() {
    // Empty
  }
}
