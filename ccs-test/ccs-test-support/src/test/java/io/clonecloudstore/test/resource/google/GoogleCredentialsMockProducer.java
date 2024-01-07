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

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import io.quarkus.test.Mock;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import org.mockito.Mockito;

@Mock
@ApplicationScoped
public class GoogleCredentialsMockProducer {

  @Produces
  @Singleton
  @Default
  public GoogleCredentials googleCredential() {
    return Mockito.mock(GoogleCredentials.class);
  }

  // only needed if you're injecting it inside one of your CDI beans
  @Produces
  @Singleton
  @Default
  public CredentialsProvider credentialsProvider() {
    return NoCredentialsProvider.create();
  }
}
