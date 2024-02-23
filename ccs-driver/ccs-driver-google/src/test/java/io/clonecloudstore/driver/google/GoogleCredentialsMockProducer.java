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

package io.clonecloudstore.driver.google;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.auth.RequestMetadataCallback;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import io.quarkus.test.Mock;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

@Mock
@ApplicationScoped
public class GoogleCredentialsMockProducer {

  @Produces
  @Singleton
  @Default
  public GoogleCredentials googleCredential() {
    return new GoogleCredentialsExtended();
  }

  private static class GoogleCredentialsExtended extends GoogleCredentials {
    private final NoCredentials credentials = NoCredentials.getInstance();

    @Override
    public GoogleCredentials createWithQuotaProject(final String quotaProject) {
      return this;
    }

    @Override
    public String getUniverseDomain() throws IOException {
      return credentials.getUniverseDomain();
    }

    @Override
    protected boolean isExplicitUniverseDomain() {
      return false;
    }

    @Override
    protected Map<String, List<String>> getAdditionalHeaders() {
      return Map.of();
    }

    @Override
    public String toString() {
      return credentials.toString();
    }

    @Override
    public boolean equals(final Object obj) {
      return true;
    }

    @Override
    public int hashCode() {
      return credentials.hashCode();
    }

    @Override
    public GoogleCredentials createScoped(final Collection<String> scopes) {
      return this;
    }

    @Override
    public GoogleCredentials createScoped(final Collection<String> scopes, final Collection<String> defaultScopes) {
      return this;
    }

    @Override
    public GoogleCredentials createScoped(final String... scopes) {
      return this;
    }

    @Override
    public GoogleCredentials createWithCustomRetryStrategy(final boolean defaultRetriesEnabled) {
      return this;
    }

    @Override
    public GoogleCredentials createDelegated(final String user) {
      return this;
    }

    @Override
    public String getAuthenticationType() {
      return credentials.getAuthenticationType();
    }

    @Override
    public boolean hasRequestMetadata() {
      return credentials.hasRequestMetadata();
    }

    @Override
    public boolean hasRequestMetadataOnly() {
      return credentials.hasRequestMetadataOnly();
    }

    @Override
    public void getRequestMetadata(final URI uri, final Executor executor, final RequestMetadataCallback callback) {
      credentials.getRequestMetadata(uri, executor, callback);
    }

    @Override
    public Map<String, List<String>> getRequestMetadata(final URI uri) throws IOException {
      return Map.of();
    }

    @Override
    public void refresh() throws IOException {
      //
    }

    @Override
    public void refreshIfExpired() throws IOException {
      //
    }

    @Override
    public AccessToken refreshAccessToken() throws IOException {
      return new AccessToken("token", new Date(System.currentTimeMillis() + 100000L));
    }

    @Override
    public Map<String, List<String>> getRequestMetadata() throws IOException {
      return Map.of();
    }
  }

  // only needed if you're injecting it inside one of your CDI beans
  @Produces
  @Singleton
  @Default
  public CredentialsProvider credentialsProvider() {
    return NoCredentialsProvider.create();
  }
}
