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

package io.clonecloudstore.accessor.client.internal;

import io.clonecloudstore.accessor.client.internal.api.AccessorObjectInternalApi;
import io.clonecloudstore.common.quarkus.client.ClientFactoryAbstract;
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Bucket Client API Factory
 */
@ApplicationScoped
@Unremovable
public class AccessorObjectInternalApiFactory extends ClientFactoryAbstract<AccessorObjectInternalApi> {
  @Override
  public AccessorObjectInternalApiClient newClient() {
    return new AccessorObjectInternalApiClient(this);
  }

  @Override
  protected Class<?> getServiceClass() {
    return AccessorObjectInternalApi.class;
  }
}
