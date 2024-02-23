/*
 * Copyright (c) 2024. Clone Cloud Store (CCS), Contributors and Frederic Bregier
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

package io.clonecloudstore.common.quarkus.example.client;

import java.io.InputStream;

import io.clonecloudstore.common.quarkus.example.model.ApiBusinessIn;
import io.clonecloudstore.common.quarkus.example.model.ApiBusinessOut;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.rest.client.inject.RestClient;

@ApplicationScoped
public class ApiQurkusNativeClient {
  @RestClient
  ApiQuarkusServiceInterface realClient;

  public ApiBusinessOut postInputStreamDirect(final ApiQuarkusClient client, final String name,
                                              final InputStream content, final long len, final boolean shallCompress,
                                              final boolean alreadyCompressed) throws CcsWithStatusException {
    // Business code should come here
    final var businessIn = new ApiBusinessIn();
    businessIn.name = name;
    businessIn.len = len;
    final var inputStream = client.prepareInputStreamToSend2(content, shallCompress, alreadyCompressed, businessIn);
    final var uni = realClient.createObject(name, len, inputStream);
    return client.getResultFromPostInputStreamUni2(uni, inputStream);
  }

}
