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

package io.clonecloudstore.test.resource.azure;

import java.util.OptionalInt;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import static io.clonecloudstore.test.resource.azure.AzureResource.DEV_SERVICE_LABEL;
import static io.clonecloudstore.test.resource.azure.AzureResource.EXPOSED_PORT;


public class AzureContainer extends GenericContainer<AzureContainer> {
  protected static final String[] ARGUMENTS =
      "azurite-blob --blobHost 0.0.0.0 --loose --skipApiVersionCheck --disableProductStyleUrl --inMemoryPersistence".split(
          " ");
  private final OptionalInt fixedExposedPort;

  public AzureContainer(DockerImageName dockerImageName, OptionalInt fixedExposedPort, String serviceName) {
    super(dockerImageName);
    this.fixedExposedPort = fixedExposedPort;

    if (serviceName != null) {
      withLabel(DEV_SERVICE_LABEL, serviceName);
    }
  }

  @Override
  protected void configure() {
    super.configure();

    if (fixedExposedPort.isPresent()) {
      addFixedExposedPort(fixedExposedPort.getAsInt(), EXPOSED_PORT);
    } else {
      addExposedPort(EXPOSED_PORT);
    }
    this.withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint(ARGUMENTS));
  }

  public int getPort() {
    if (fixedExposedPort.isPresent()) {
      return fixedExposedPort.getAsInt();
    }
    return super.getFirstMappedPort();
  }
}
