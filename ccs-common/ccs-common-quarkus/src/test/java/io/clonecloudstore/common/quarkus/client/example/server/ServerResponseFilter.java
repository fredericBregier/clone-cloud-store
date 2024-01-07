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

package io.clonecloudstore.common.quarkus.client.example.server;

import java.io.IOException;

import io.clonecloudstore.common.quarkus.client.SimpleClientAbstract;
import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.common.standard.system.ParametersChecker;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;
import org.jboss.logging.Logger;

import static io.clonecloudstore.common.quarkus.client.SimpleClientAbstract.X_MODULE;
import static io.clonecloudstore.common.quarkus.client.SimpleClientAbstract.X_OP_ID;

@Provider
public class ServerResponseFilter implements ContainerResponseFilter {
  private static final Logger LOGGER = Logger.getLogger(ServerResponseFilter.class);

  @Override
  public void filter(final ContainerRequestContext containerRequestContext,
                     final ContainerResponseContext responseContext) throws IOException {
    if (ParametersChecker.isEmpty(responseContext.getHeaderString(X_OP_ID))) {
      responseContext.getHeaders().putSingle(X_OP_ID, SimpleClientAbstract.getMdcOpId());
    }
    if (ParametersChecker.isEmpty(responseContext.getHeaderString(X_MODULE))) {
      responseContext.getHeaders().putSingle(X_MODULE, QuarkusProperties.getCcsModule().name());
    }
  }
}
