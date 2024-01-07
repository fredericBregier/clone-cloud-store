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

package io.clonecloudstore.accessor.client;

import java.net.URI;

import io.clonecloudstore.accessor.client.api.AccessorBucketApi;
import io.clonecloudstore.common.quarkus.client.SimpleClientFactoryAbstract;
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Bucket Client API Factory
 */
@ApplicationScoped
@Unremovable
public class AccessorBucketApiFactory extends SimpleClientFactoryAbstract<AccessorBucketApi> {
  @Override
  public AccessorBucketApiClient newClient() {
    return new AccessorBucketApiClient(this);
  }

  @Override
  public synchronized AccessorBucketApiClient newClient(final URI uri) {
    return (AccessorBucketApiClient) super.newClient(uri);
  }

  @Override
  protected Class<?> getServiceClass() {
    return AccessorBucketApi.class;
  }
}
