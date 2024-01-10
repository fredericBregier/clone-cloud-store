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

package io.clonecloudstore.common.standard.inputstream;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.clonecloudstore.common.standard.properties.StandardProperties;
import io.clonecloudstore.common.standard.system.SysErrLogger;
import io.clonecloudstore.common.standard.system.SystemRandomSecure;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.test.stream.FakeInputStream;
import io.clonecloudstore.test.stream.VoidOutputStream;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@QuarkusTest
class CipherInputStreamTest {

  private IvParameterSpec initIv() {
    final var secureRandom = SystemRandomSecure.getSecureRandomSingleton();
    final var iv = new byte[16];
    secureRandom.nextBytes(iv);
    return new IvParameterSpec(iv);
  }

  private SecretKeySpec initSecret() throws NoSuchAlgorithmException, InvalidKeySpecException {
    final var secretKeyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
    final var pbeKeySpec =
        new PBEKeySpec("PassPhrase".toCharArray(), "salt".getBytes(StandardCharsets.UTF_8), 65536, 128);
    final var secretKey = secretKeyFactory.generateSecret(pbeKeySpec);
    return new SecretKeySpec(secretKey.getEncoded(), "AES");
  }

  private Cipher initCipher(final SecretKeySpec secret, final IvParameterSpec iv, final boolean encrypt)
      throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidAlgorithmParameterException, InvalidKeyException {
    final var cipherEnc = Cipher.getInstance("AES/CBC/NoPadding");
    cipherEnc.init(encrypt ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE, secret, iv);
    return cipherEnc;
  }

  @Test
  void test50InputStreamAndCipher()
      throws IOException, InterruptedException, NoSuchAlgorithmException, InvalidKeySpecException,
      NoSuchPaddingException, InvalidAlgorithmParameterException, InvalidKeyException {
    final var len = 100 * 1024 * 1024;
    try (final var inputStream = new FakeInputStream(len)) {
      final var countDownLatch = new CountDownLatch(1);
      final var computedLen = new AtomicLong(0);
      final var start = System.nanoTime();

      // Cipher
      final var iv = initIv();
      final var secret = initSecret();
      final var cipherEnc = initCipher(secret, iv, true);
      final var cipherDec = initCipher(secret, iv, false);
      final var cipherEncrypt = new CipherInputStream(inputStream, cipherEnc);

      final AtomicReference<IOException> ioExceptionAtomicReference = new AtomicReference<>();
      final var decrypt = new CipherInputStream(cipherEncrypt, cipherDec);
      final OutputStream outputStream = new VoidOutputStream();
      SystemTools.STANDARD_EXECUTOR_SERVICE.execute(() -> {
        try {
          decrypt.available();
          computedLen.set(decrypt.transferTo(outputStream));
        } catch (final IOException e) {
          ioExceptionAtomicReference.set(e);
        } finally {
          countDownLatch.countDown();
        }
      });
      countDownLatch.await();
      final var stop = System.nanoTime();
      Thread.yield();
      if (ioExceptionAtomicReference.get() != null) {
        assertTrue(computedLen.get() >= decrypt.getSizeRead());
        fail(ioExceptionAtomicReference.get());
      } else {
        assertEquals(len, computedLen.get());
      }
      SysErrLogger.FAKE_LOGGER.sysout(" MB/s " + len / ((stop - start) / 1000000000.0) / (1024 * 1024.0));
      SysErrLogger.FAKE_LOGGER.sysout("Len: " + cipherEncrypt.getSizeRead() + " vs " + decrypt.getSizeCipher());
      SysErrLogger.FAKE_LOGGER.sysout(cipherEncrypt);
      SysErrLogger.FAKE_LOGGER.sysout(decrypt);
      assertEquals(-1, cipherEncrypt.read());
      assertEquals(0, cipherEncrypt.skip(StandardProperties.getBufSize()));
      assertEquals(0, cipherEncrypt.transferTo(new VoidOutputStream()));
      assertEquals(0, cipherEncrypt.available());
      assertEquals(-1, decrypt.read());
      assertEquals(0, decrypt.skip(StandardProperties.getBufSize()));
      assertEquals(0, decrypt.transferTo(new VoidOutputStream()));
      assertEquals(0, decrypt.available());
      cipherEncrypt.close();
      decrypt.close();
    }
  }

  @Test
  void test51InputStreamAndCipherHalf()
      throws IOException, InterruptedException, NoSuchAlgorithmException, InvalidKeySpecException,
      NoSuchPaddingException, InvalidAlgorithmParameterException, InvalidKeyException {
    final var len = 100 * 1024 * 1024;
    try (final var inputStream = new FakeInputStream(len)) {
      final var countDownLatch = new CountDownLatch(1);
      final var computedLen = new AtomicLong(0);
      final var start = System.nanoTime();

      // Cipher
      final var iv = initIv();
      final var secret = initSecret();
      final var cipherEnc = initCipher(secret, iv, true);
      final var cipherEncrypt = new CipherInputStream(inputStream, cipherEnc);
      final AtomicReference<IOException> ioExceptionAtomicReference = new AtomicReference<>();

      final OutputStream outputStream = new VoidOutputStream();
      SystemTools.STANDARD_EXECUTOR_SERVICE.execute(() -> {
        try {
          cipherEncrypt.available();
          computedLen.set(cipherEncrypt.transferTo(outputStream));
        } catch (final IOException e) {
          ioExceptionAtomicReference.set(e);
        } finally {
          countDownLatch.countDown();
        }
      });
      countDownLatch.await();
      final var stop = System.nanoTime();
      Thread.yield();
      if (ioExceptionAtomicReference.get() != null) {
        assertTrue(computedLen.get() >= cipherEncrypt.getSizeRead());
        fail(ioExceptionAtomicReference.get());
      } else {
        assertEquals(len, computedLen.get());
      }
      SysErrLogger.FAKE_LOGGER.sysout(" MB/s " + len / ((stop - start) / 1000000000.0) / (1024 * 1024.0));
      SysErrLogger.FAKE_LOGGER.sysout("Len: " + cipherEncrypt.getSizeRead() + " vs " + cipherEncrypt.getSizeCipher());
      assertEquals(-1, cipherEncrypt.read());
      assertEquals(0, cipherEncrypt.skip(StandardProperties.getBufSize()));
      assertEquals(0, cipherEncrypt.transferTo(new VoidOutputStream()));
      assertEquals(0, cipherEncrypt.available());
      assertEquals(-1, inputStream.read());
      cipherEncrypt.close();
    }
  }

}
