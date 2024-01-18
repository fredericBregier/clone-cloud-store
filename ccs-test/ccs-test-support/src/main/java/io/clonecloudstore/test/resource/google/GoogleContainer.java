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

package io.clonecloudstore.test.resource.google;

import java.util.OptionalInt;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import static io.clonecloudstore.test.resource.google.GoogleResource.EXPOSED_PORT;

public class GoogleContainer extends GenericContainer<GoogleContainer> {
  protected static final String[] ARGUMENTS =
      "/bin/fake-gcs-server -scheme http -log-level warn -backend filesystem".split(" ");
  private final OptionalInt fixedExposedPort;

  public GoogleContainer(DockerImageName dockerImageName, OptionalInt fixedExposedPort) {
    super(dockerImageName);
    this.fixedExposedPort = fixedExposedPort;
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
