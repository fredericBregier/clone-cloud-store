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

package io.clonecloudstore.administration.database.model;

import java.util.List;

import io.clonecloudstore.administration.model.Topology;
import io.clonecloudstore.administration.model.TopologyStatus;
import io.clonecloudstore.common.database.utils.DbQuery;
import io.clonecloudstore.common.database.utils.DbUpdate;
import io.clonecloudstore.common.database.utils.RepositoryBaseInterface;
import io.clonecloudstore.common.database.utils.RestQuery;
import io.clonecloudstore.common.database.utils.exception.CcsDbException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;

public interface DaoTopologyRepository extends RepositoryBaseInterface<DaoTopology> {
  String TABLE_NAME = "topologies";
  String URI = "uri";
  String NAME = "name";
  String STATUS = "status";

  default Topology insertTopology(final Topology topology) throws CcsDbException {
    try {
      final var daoTopology = this.createEmptyItem().fromDto(topology);
      this.insert(daoTopology);
      return daoTopology.getDto();
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  default Topology findBySite(final String site) throws CcsDbException {
    try {
      final var dao = this.findWithPk(site);
      return (dao != null ? dao.getDto() : null);
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  default List<Topology> findAllTopologies() throws CcsDbException {
    try {
      return findStream(new DbQuery()).map(DaoTopology::getDto).toList();
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  default List<Topology> findTopologies(final TopologyStatus status) throws CcsDbException {
    try {
      return findStream(new DbQuery(RestQuery.QUERY.EQ, STATUS, status)).map(DaoTopology::getDto).toList();
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  default Topology updateTopologyStatus(final Topology topology, final TopologyStatus status) throws CcsDbException {
    try {
      final var query = new DbQuery(RestQuery.QUERY.EQ, getPkName(), topology.id());
      final var dbUpdate = new DbUpdate().set(STATUS, status);

      if (this.update(query, dbUpdate) != 1) {
        throw new CcsDbException(new CcsOperationException("Could not update topology"));
      }
      return new Topology(topology, status);
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  default Topology updateTopology(final Topology topology) throws CcsDbException {
    try {
      final var query = new DbQuery(RestQuery.QUERY.EQ, getPkName(), topology.id());
      final var dbUpdate =
          new DbUpdate().set(STATUS, topology.status()).set(NAME, topology.name()).set(URI, topology.uri());

      if (this.update(query, dbUpdate) != 1) {
        throw new CcsDbException(new CcsOperationException("Could not update topology"));
      }
      return topology;
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }

  default boolean deleteTopology(final String site) throws CcsDbException {
    try {
      final var query = new DbQuery(RestQuery.QUERY.EQ, getPkName(), site);
      if (this.delete(query) != 1) {
        throw new CcsDbException(new CcsOperationException("Could not delete topology"));
      } else {
        return true;
      }
    } catch (final RuntimeException e) {
      throw new CcsDbException(e);
    }
  }
}
