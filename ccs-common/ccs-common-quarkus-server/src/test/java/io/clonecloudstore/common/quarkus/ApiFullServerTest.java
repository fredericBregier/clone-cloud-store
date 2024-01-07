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

package io.clonecloudstore.common.quarkus;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

import io.clonecloudstore.common.quarkus.example.WaitingInputStream;
import io.clonecloudstore.common.quarkus.example.client.ApiQuarkusClientFactory;
import io.clonecloudstore.common.quarkus.example.model.ApiBusinessOut;
import io.clonecloudstore.common.quarkus.example.server.ApiQuarkusService;
import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.common.standard.exception.CcsWithStatusException;
import io.clonecloudstore.common.standard.inputstream.CipherInputStream;
import io.clonecloudstore.common.standard.inputstream.MultipleActionsInputStream;
import io.clonecloudstore.common.standard.inputstream.ZstdCompressInputStream;
import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.system.SysErrLogger;
import io.clonecloudstore.common.standard.system.SystemRandomSecure;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.clonecloudstore.test.stream.VoidOutputStream;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MediaType;
import org.jboss.logging.Logger;
import org.jboss.resteasy.reactive.ClientWebApplicationException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static io.clonecloudstore.common.quarkus.client.SimpleClientAbstract.X_OP_ID;
import static io.clonecloudstore.common.quarkus.example.client.ApiConstants.API_COLLECTIONS;
import static io.clonecloudstore.common.quarkus.example.client.ApiConstants.API_FULLROOT;
import static io.clonecloudstore.common.quarkus.example.client.ApiConstants.X_NAME;
import static io.clonecloudstore.common.quarkus.example.server.ApiQuarkusService.CIPHER;
import static io.clonecloudstore.common.quarkus.example.server.ApiQuarkusService.PROXY_TEST;
import static io.clonecloudstore.common.quarkus.example.server.ApiQuarkusService.THROWABLE_NAME;
import static io.clonecloudstore.common.quarkus.example.server.ApiQuarkusService.ULTRA_COMPRESSION_TEST;
import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

@TestMethodOrder(MethodOrderer.MethodName.class)
@QuarkusTest
class ApiFullServerTest {
  private static final Logger LOG = Logger.getLogger(ApiFullServerTest.class);

  private static boolean waitForBadPost = false;
  @Inject
  ApiQuarkusClientFactory factory;

  @BeforeAll
  static void beforeAll() throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidAlgorithmParameterException,
      NoSuchPaddingException, InvalidKeyException {
    QuarkusProperties.setServerComputeSha256(false);
    final var iv = initIv();
    final var secret = initSecret();
    ApiQuarkusService.cipherEnc = initCipher(secret, iv, true);
    ApiQuarkusService.cipherDec = initCipher(secret, iv, false);
  }

  @AfterAll
  static void afterAll() throws InterruptedException {
    if (waitForBadPost) {
      Thread.sleep(5000);
    }
  }

  private static IvParameterSpec initIv() {
    final var secureRandom = SystemRandomSecure.getSecureRandomSingleton();
    final var iv = new byte[16];
    secureRandom.nextBytes(iv);
    return new IvParameterSpec(iv);
  }

  private static SecretKeySpec initSecret() throws NoSuchAlgorithmException, InvalidKeySpecException {
    final var secretKeyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
    final var pbeKeySpec =
        new PBEKeySpec("PassPhrase".toCharArray(), "salt".getBytes(StandardCharsets.UTF_8), 65536, 128);
    final var secretKey = secretKeyFactory.generateSecret(pbeKeySpec);
    return new SecretKeySpec(secretKey.getEncoded(), "AES");
  }

  private static Cipher initCipher(final SecretKeySpec secret, final IvParameterSpec iv, final boolean encrypt)
      throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidAlgorithmParameterException, InvalidKeyException {
    final var cipherEnc = Cipher.getInstance("AES/CBC/NoPadding");
    cipherEnc.init(encrypt ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE, secret, iv);
    return cipherEnc;
  }

  @BeforeEach
  void beforeEach() {
    QuarkusProperties.setServerComputeSha256(false);
    try {
      Thread.sleep(10);
    } catch (final InterruptedException e) {
      // Ignore
    }
  }

  @Test
  void check02CallInError() {
    try (final var client = factory.newClient()) {
      final var businessOut = client.getObjectMetadata(ApiQuarkusService.NOT_FOUND_NAME);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    }
    try (final var client = factory.newClient()) {
      final var businessOut = client.getObjectMetadata(ApiQuarkusService.CONFLICT_NAME);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    }
    try (final var client = factory.newClient()) {
      final var businessOut = client.getObjectMetadata(ApiQuarkusService.NOT_ACCEPTABLE_NAME);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    }
    try (final var client = factory.newClient()) {
      final var out = client.getInputStream(THROWABLE_NAME, 0, false, false);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    }
    try (final var client = factory.newClient()) {
      final var out = client.postInputStream(THROWABLE_NAME, new FakeInputStream(100), 0, false, false);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    }
    try (final var client = factory.newClient()) {
      final var out = client.getInputStreamThrough(THROWABLE_NAME, 0, false, false);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    } catch (ClientWebApplicationException e) {
      LOG.info("Error received", e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      final var out = client.postInputStreamThrough(THROWABLE_NAME, new FakeInputStream(100), 0, false, false);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    } catch (Throwable e) {
      LOG.info("Error received", e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      final var out = client.putAsGetInputStream(THROWABLE_NAME, 0, false, false);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    } catch (ClientWebApplicationException e) {
      LOG.info("Error received", e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      final var out = client.putInputStream(THROWABLE_NAME, new FakeInputStream(100), 0, false, false);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    } catch (Throwable e) {
      LOG.info("Error received", e);
      fail(e);
    }
  }

  @Test
  void check03GetObjectMetadata() {
    try (final var client = factory.newClient()) {
      final var businessOut = client.getObjectMetadata("test");
      assertEquals("test", businessOut.name);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
  }

  @Test
  void check10PostInputStreamQuarkus() {
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream("test", new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), ApiQuarkusService.LEN,
              false, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream("test", new FakeInputStream(ApiQuarkusService.LEN), ApiQuarkusService.LEN, true,
              false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream("test", new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), ApiQuarkusService.LEN,
              true, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut = client.postInputStream("test",
          new ZstdCompressInputStream(new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A')), ApiQuarkusService.LEN,
          true, true);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream("test", new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), ApiQuarkusService.LEN,
              false, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut = client.postInputStream("test" + CIPHER,
          new CipherInputStream(new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), ApiQuarkusService.cipherEnc),
          ApiQuarkusService.LEN, false, false);
      assertEquals("test" + CIPHER, businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  @Test
  void check10PostInputStreamQuarkusDouble() {
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(PROXY_TEST + "test", new FakeInputStream(ApiQuarkusService.LEN), ApiQuarkusService.LEN,
              false, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  @Test
  void check11PostInputStreamQuarkusNoSize() {
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream("test", new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), 0, false, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream("test", new FakeInputStream(ApiQuarkusService.LEN), 0, true, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream("test", new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), 0, true, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  @Test
  void check04PostInputStreamRestassuredNotChunked() {
    final var len = 5 * 1024 * 1024; // Max 10 MB by default
    {
      // Content-Length set to empty or -1
      final var start = System.nanoTime();
      final var businessOut =
          given().header(X_NAME, "test").contentType(MediaType.APPLICATION_OCTET_STREAM).header(X_OP_ID, "1")
              .body(new FakeInputStream(len)).when().post(API_FULLROOT + API_COLLECTIONS).then().statusCode(201)
              .extract().as(ApiBusinessOut.class);
      assertEquals("test", businessOut.name);
      assertEquals(len, businessOut.len);
      assertNotNull(businessOut.creationDate);
      final var stop = System.nanoTime();
      LOG.info("Speed (MB/s): " + ((float) len) / 1024.0 / 1024.0 / ((stop - start) / 1000000000.0));
    }
  }

  @Test
  void check12PostInputStreamQuarkusOkThenWrong() {
    final var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream("test", new FakeInputStream(ApiQuarkusService.LEN), ApiQuarkusService.LEN, true,
              false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
      final var stop = System.nanoTime();
      LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    try (final var client = factory.newClient()) {
      LOG.info("next shall be in error");
      try {
        final var businessOut2 =
            client.postInputStream(ApiQuarkusService.CONFLICT_NAME, new FakeInputStream(ApiQuarkusService.LEN),
                ApiQuarkusService.LEN, true, false);
        fail("Should raised an exception!");
      } catch (final CcsWithStatusException e) {
        LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
      }
      LOG.info("next shall be in error");
      try {
        final var businessOut2 = client.postInputStream(ApiQuarkusService.CONFLICT_NAME,
            new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), ApiQuarkusService.LEN, true, false);
        fail("Should raised an exception!");
      } catch (final CcsWithStatusException e) {
        LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
      }
    }
  }

  @Test
  void check13PostInputStreamQuarkusOkThenWrongNoSize() {
    final var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream("test", new FakeInputStream(ApiQuarkusService.LEN), 0, true, false);
      assertEquals("test", businessOut.name);
      Assertions.assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
      final var stop = System.nanoTime();
      LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
      LOG.info("next shall be in error");
      try {
        final var businessOut2 =
            client.postInputStream(ApiQuarkusService.CONFLICT_NAME, new FakeInputStream(ApiQuarkusService.LEN), 0, true,
                false);
        fail("Should raised an exception!");
      } catch (final CcsWithStatusException e) {
        LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
      }
      LOG.info("next shall be in error");
      try {
        final var businessOut2 = client.postInputStream(ApiQuarkusService.CONFLICT_NAME,
            new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), 0, true, false);
        fail("Should raised an exception!");
      } catch (final CcsWithStatusException e) {
        LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
      }
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
  }

  @Test
  void check14PostInputStreamQuarkusSha() {
    QuarkusProperties.setServerComputeSha256(true);
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream("test", new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), ApiQuarkusService.LEN,
              false, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream("test", new FakeInputStream(ApiQuarkusService.LEN), ApiQuarkusService.LEN, true,
              false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream("test", new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), ApiQuarkusService.LEN,
              true, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  @Test
  void check15PostInputStreamQuarkusShaNoSize() {
    QuarkusProperties.setServerComputeSha256(true);
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream("test", new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), 0, false, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream("test", new FakeInputStream(ApiQuarkusService.LEN), 0, true, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream("test", new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), 0, true, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  @Test
  void check16GetInputStreamQuarkus() {
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut = client.getInputStream("test", ApiQuarkusService.LEN, false, false);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertNotNull(inputStreamBusinessOut.dtoOut());
      assertEquals("test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
      LOG.infof("IS: %s", inputStreamBusinessOut.inputStream());
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut = client.getInputStream("test", ApiQuarkusService.LEN, true, true);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertNotNull(inputStreamBusinessOut.dtoOut());
      assertEquals("test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
      LOG.infof("IS: %s", inputStreamBusinessOut.inputStream());
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut =
          client.getInputStream(ULTRA_COMPRESSION_TEST + "test", ApiQuarkusService.LEN, true, true);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertNotNull(inputStreamBusinessOut.dtoOut());
      assertEquals(ULTRA_COMPRESSION_TEST + "test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
      LOG.infof("IS: %s", inputStreamBusinessOut.inputStream());
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut =
          client.getInputStream(ULTRA_COMPRESSION_TEST + "test", ApiQuarkusService.LEN, true, false);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertNotNull(inputStreamBusinessOut.dtoOut());
      assertEquals(ULTRA_COMPRESSION_TEST + "test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
      LOG.infof("IS: %s", inputStreamBusinessOut.inputStream());
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut = client.getInputStream("test", ApiQuarkusService.LEN, false, false);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertNotNull(inputStreamBusinessOut.dtoOut());
      assertEquals("test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
      LOG.infof("IS: %s", inputStreamBusinessOut.inputStream());
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut = client.getInputStream("test" + CIPHER, ApiQuarkusService.LEN, false, false);
      final var inp = new CipherInputStream(inputStreamBusinessOut.inputStream(), ApiQuarkusService.cipherDec);
      final var len = inp.transferTo(new VoidOutputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertNotNull(inputStreamBusinessOut.dtoOut());
      assertEquals("test" + CIPHER, inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
      LOG.infof("IS: %s", inputStreamBusinessOut.inputStream());
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  @Test
  void check16GetInputStreamQuarkusDouble() {
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut =
          client.getInputStream(PROXY_TEST + "test", ApiQuarkusService.LEN, false, false);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      if (inputStreamBusinessOut.inputStream() instanceof MultipleActionsInputStream mai) {
        LOG.infof("DEBUG %s", mai);
      }
      assertEquals(ApiQuarkusService.LEN, len);
      assertEquals("test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
      LOG.infof("IS: %s", inputStreamBusinessOut.inputStream());
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  @Test
  void check17GetInputStreamQuarkusNoSize() {
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut = client.getInputStream("test", 0, false, false);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertEquals("test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut = client.getInputStream("test", 0, true, true);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertEquals("test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut = client.getInputStream(ULTRA_COMPRESSION_TEST + "test", 0, true, true);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertEquals(ULTRA_COMPRESSION_TEST + "test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  @Test
  void check18GetInputStreamQuarkusOkThenWrong() {
    final var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut = client.getInputStream("test", ApiQuarkusService.LEN, true, true);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertEquals("test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
      final var stop = System.nanoTime();
      LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
      LOG.info("next shall be in error");
      try {
        final var inputStreamBusinessOut1 =
            client.getInputStream(ApiQuarkusService.NOT_FOUND_NAME, ApiQuarkusService.LEN, true, true);
        final var len2 = FakeInputStream.consumeAll(inputStreamBusinessOut1.inputStream());
        fail("Should raised an exception!");
      } catch (final CcsWithStatusException e) {
        LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
      } catch (final IOException e) {
        LOG.warn("Error received: " + e.getMessage());
      }
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
  }

  @Test
  void check19GetInputStreamQuarkusOkThenWrongNoSize() {
    final var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut = client.getInputStream("test", 0, true, true);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertEquals("test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
      final var stop = System.nanoTime();
      LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
      LOG.info("next shall be in error");
      try {
        final var inputStreamBusinessOut1 = client.getInputStream(ApiQuarkusService.NOT_FOUND_NAME, 0, true, true);
        final var len2 = FakeInputStream.consumeAll(inputStreamBusinessOut1.inputStream());
        fail("Should raised an exception!");
      } catch (final CcsWithStatusException e) {
        LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
      } catch (final IOException e) {
        LOG.warn("Error received: " + e.getMessage());
      }
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
  }

  @Test
  void check19_1_GetInputStreamQuarkusOkThenWrongDelay() {
    final var start = System.nanoTime();
    final var oldDelay = StandardProperties.getMaxWaitMs();
    try (final var client = factory.newClient()) {
      StandardProperties.setMaxWaitMs(200);
      final var inputStreamBusinessOut = client.getInputStream("test", 0, true, true);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertEquals("test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
      final var stop = System.nanoTime();
      LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
      LOG.info("next shall be in error");
      try {
        final var inputStreamBusinessOut1 =
            client.getInputStream(ApiQuarkusService.DELAY_TEST + "test", 0, false, false);
        Thread.sleep(500);
        final var len2 = FakeInputStream.consumeAll(inputStreamBusinessOut1.inputStream());
        //assertNotEquals(ApiQuarkusService.LEN, len2);
        LOG.warnf(SysErrLogger.red("Should raised an exception! %b"), ApiQuarkusService.LEN == len2);
      } catch (final CcsWithStatusException e) {
        LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
      } catch (final IOException e) {
        LOG.warn("Error received: " + e.getMessage());
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    } finally {
      StandardProperties.setMaxWaitMs(oldDelay);
    }
  }

  @Test
  void check20PutAsPostInputStreamQuarkus() {
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.putInputStream("test", new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), ApiQuarkusService.LEN,
              false, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.putInputStream("test", new FakeInputStream(ApiQuarkusService.LEN), ApiQuarkusService.LEN, true, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.putInputStream("test", new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), ApiQuarkusService.LEN,
              true, false);
      assertEquals("test", businessOut.name);
      assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  @Test
  void check20putAsGetInputStreamQuarkus() {
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut = client.putAsGetInputStream("test", ApiQuarkusService.LEN, false, false);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertEquals("test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    var stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut = client.putAsGetInputStream("test", ApiQuarkusService.LEN, true, true);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertEquals("test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut =
          client.putAsGetInputStream(ULTRA_COMPRESSION_TEST + "test", ApiQuarkusService.LEN, true, true);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      assertEquals(ApiQuarkusService.LEN, len);
      assertEquals(ULTRA_COMPRESSION_TEST + "test", inputStreamBusinessOut.dtoOut().name);
      assertEquals(ApiQuarkusService.LEN, inputStreamBusinessOut.dtoOut().len);
    } catch (final CcsWithStatusException | IOException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    }
    stop = System.nanoTime();
    LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
  }

  @Test
  void check21WrongPostInputStreamQuarkus() {
    final var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(ApiQuarkusService.CONFLICT_NAME, new FakeInputStream(ApiQuarkusService.LEN),
              ApiQuarkusService.LEN, false, false);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    }
    final var stop = System.nanoTime();
    LOG.info("Speed (ms): " + ((stop - start) / 1000000.0));
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(ApiQuarkusService.CONFLICT_NAME, new FakeInputStream(ApiQuarkusService.LEN),
              ApiQuarkusService.LEN, true, false);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    }
    try (final var client = factory.newClient()) {
      final var businessOut = client.postInputStream(ApiQuarkusService.CONFLICT_NAME,
          new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), ApiQuarkusService.LEN, true, false);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    }
  }

  @Test
  void check22WrongPostInputStreamQuarkusNoSize() {
    final var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(ApiQuarkusService.CONFLICT_NAME, new FakeInputStream(ApiQuarkusService.LEN), 0, false,
              false);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    }
    final var stop = System.nanoTime();
    LOG.info("Speed (ms): " + ((stop - start) / 1000000.0));
    try (final var client = factory.newClient()) {
      final var businessOut =
          client.postInputStream(ApiQuarkusService.CONFLICT_NAME, new FakeInputStream(ApiQuarkusService.LEN), 0, true,
              false);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    }
    try (final var client = factory.newClient()) {
      final var businessOut = client.postInputStream(ApiQuarkusService.CONFLICT_NAME,
          new FakeInputStream(ApiQuarkusService.LEN, (byte) 'A'), 0, true, false);
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    }
  }

  @Test
  void check23WrongGetInputStreamQuarkus() {
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut =
          client.getInputStream(ApiQuarkusService.NOT_FOUND_NAME, ApiQuarkusService.LEN, false, false);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    } catch (final IOException e) {
      LOG.warn("Error received: " + e.getMessage());
    }
    var stop = System.nanoTime();
    LOG.info("Speed (ms): " + ((stop - start) / 1000000.0));
    // Redo to check multiple access
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut =
          client.getInputStream(ApiQuarkusService.NOT_FOUND_NAME, ApiQuarkusService.LEN, true, true);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    } catch (final IOException e) {
      LOG.warn("Error received: " + e.getMessage());
    }
    stop = System.nanoTime();
    LOG.info("Speed (ms): " + ((stop - start) / 1000000.0));
  }

  @Test
  void check24WrongGetInputStreamQuarkusNoSize() {
    var start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut = client.getInputStream(ApiQuarkusService.NOT_FOUND_NAME, 0, false, false);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    } catch (final IOException e) {
      LOG.warn("Error received: " + e.getMessage());
    }
    var stop = System.nanoTime();
    LOG.info("Speed (ms): " + ((stop - start) / 1000000.0));
    // Redo to check multiple access
    start = System.nanoTime();
    try (final var client = factory.newClient()) {
      final var inputStreamBusinessOut = client.getInputStream(ApiQuarkusService.NOT_FOUND_NAME, 0, true, true);
      final var len = FakeInputStream.consumeAll(inputStreamBusinessOut.inputStream());
      fail("Should raised an exception!");
    } catch (final CcsWithStatusException e) {
      LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
    } catch (final IOException e) {
      LOG.warn("Error received: " + e.getMessage());
    }
    stop = System.nanoTime();
    LOG.info("Speed (ms): " + ((stop - start) / 1000000.0));
  }

  @Test
  void check50_PostInputStreamNettyOkNoWrongDelay() {
    final var start = System.nanoTime();
    final var oldTimeOut = StandardProperties.getMaxWaitMs();
    try (final var client = factory.newClient()) {
      StandardProperties.setMaxWaitMs(200);
      final var businessOut =
          client.postInputStream("test", new FakeInputStream(ApiQuarkusService.LEN), 0, true, false);
      assertEquals("test", businessOut.name);
      Assertions.assertEquals(ApiQuarkusService.LEN, businessOut.len);
      assertNotNull(businessOut.creationDate);
      final var stop = System.nanoTime();
      LOG.info("Speed (MB/s): " + ApiQuarkusService.LEN / 1024 / 1024.0 / ((stop - start) / 1000000000.0));
    } catch (final CcsWithStatusException e) {
      LOG.error(e.getMessage(), e);
      fail(e);
    } finally {
      StandardProperties.setMaxWaitMs(oldTimeOut);
    }
  }

  @Test
    //@Disabled("Long stop")
  void check50_PostInputStreamQuarkusOkThenWrongDelay() {
    final var oldTimeOut = StandardProperties.getMaxWaitMs();
    final var oldClientResponse = QuarkusProperties.clientResponseTimeOut();
    try (final var client = factory.newClient()) {
      StandardProperties.setMaxWaitMs(200);
      QuarkusProperties.setClientResponseTimeOut(100);
      LOG.info("next shall be in error");
      try {
        final var businessOut2 =
            client.postInputStream("test2", new WaitingInputStream(1024 * 1024, 10), 0, true, false);
        LOG.infof("Get %s", businessOut2);
        fail("Should raised an exception!");
      } catch (final CcsWithStatusException e) {
        LOG.info("Error received: " + e.getStatus() + " => " + e.getMessage());
      }
    } finally {
      StandardProperties.setMaxWaitMs(oldTimeOut);
      QuarkusProperties.setClientResponseTimeOut(oldClientResponse);
    }
  }
}
