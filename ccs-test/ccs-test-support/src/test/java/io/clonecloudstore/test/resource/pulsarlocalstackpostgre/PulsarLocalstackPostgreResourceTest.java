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

package io.clonecloudstore.test.resource.pulsarlocalstackpostgre;

import java.net.URI;
import java.net.URISyntaxException;

import io.clonecloudstore.test.resource.ResourcesConstants;
import io.clonecloudstore.test.resource.kafka.TopicConsumer;
import io.clonecloudstore.test.resource.kafka.TopicProducer;
import io.clonecloudstore.test.resource.postgre.PgPersonEntity;
import io.clonecloudstore.test.resource.postgre.PgPersonRepository;
import io.clonecloudstore.test.resource.postgres.PostgresResource;
import io.clonecloudstore.test.resource.pulsar.PulsarResource;
import io.clonecloudstore.test.resource.s3.EmptyClass;
import io.clonecloudstore.test.resource.s3.LocalStackResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
@TestProfile(PulsarLocalstackPostgreProfile.class)
class PulsarLocalstackPostgreResourceTest {

  @Inject
  TopicConsumer topicConsumer;
  @Inject
  TopicProducer topicProducer;
  @Inject
  PgPersonRepository repository;

  @Test
  @Transactional
  void checkDb() {
    assertNotNull(PostgresResource.getJdbcUrl());
    System.out.println(PostgresResource.getJdbcUrl());
    assertEquals(0, repository.count());
    final var personEntity = new PgPersonEntity();
    personEntity.setName("name");
    repository.persistAndFlush(personEntity);
    assertEquals(1, repository.count());
    final var personEntity1 = repository.findByName("name");
    assertEquals(personEntity.getName(), personEntity1.getName());
    assertEquals(personEntity.getId(), personEntity1.getId());
    repository.delete(personEntity);
    assertEquals(0, repository.count());
  }

  @Test
  void checkConfiguration() {
    assertNotNull(PulsarResource.getPulsarBrokerUrl());
    assertNotNull(PostgresResource.getJdbcUrl());
    assertNotNull(PostgresResource.getDatabaseName());
    assertNotNull(PostgresResource.getUsername());
    assertNotNull(PostgresResource.getPassword());
    assertEquals("true",
        ConfigProvider.getConfig().getValue(ResourcesConstants.QUARKUS_HIBERNATE_ORM_ENABLED, String.class));
  }

  @Test
  void checkTopic() {
    topicProducer.send(12);
    assertDoesNotThrow(() -> topicConsumer.waitProcess());
  }

  @Test
  void s3Check() throws URISyntaxException {
    new EmptyClass();
    final var bucket = "namebucket";
    var url = LocalStackResource.getUrlString();
    if (url.contains("localhost")) {
      url = url.replace("localhost", "127.0.0.1");
    }
    try (final var s3Client = S3Client.builder().endpointOverride(new URI(url)).credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(LocalStackResource.getAccessKey(), LocalStackResource.getSecretKey())))
        .region(Region.of(LocalStackResource.getRegion())).build()) {
      final var bucketRequestExist = HeadBucketRequest.builder().bucket(bucket).build();
      try {
        s3Client.headBucket(bucketRequestExist).sdkHttpResponse().isSuccessful();
        fail("Should raised an exception");
      } catch (final NoSuchBucketException ignored) {
        // Ignore
      }
      final var bucketRequest = CreateBucketRequest.builder().bucket(bucket).build();
      final var response = s3Client.createBucket(bucketRequest);
      assertTrue(response.sdkHttpResponse().isSuccessful());
      final var bucketRequestWait = HeadBucketRequest.builder().bucket(bucket).build();
      // Wait until the bucket is created
      final var s3Waiter = s3Client.waiter();
      final var waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
      assertFalse(waiterResponse.matched().response().isEmpty() ||
          !waiterResponse.matched().response().get().sdkHttpResponse().isSuccessful());
      try {
        s3Client.headBucket(bucketRequestExist).sdkHttpResponse().isSuccessful();
      } catch (final NoSuchBucketException e) {
        fail(e);
      }
      final var responseDelete = s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build());
      assertTrue(response.sdkHttpResponse().isSuccessful());
    }
  }
}
