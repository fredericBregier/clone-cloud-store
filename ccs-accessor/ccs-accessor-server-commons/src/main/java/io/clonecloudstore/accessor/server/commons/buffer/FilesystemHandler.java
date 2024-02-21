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

package io.clonecloudstore.accessor.server.commons.buffer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.type.TypeReference;
import io.clonecloudstore.common.quarkus.metrics.BulkMetrics;
import io.clonecloudstore.common.quarkus.modules.AccessorProperties;
import io.clonecloudstore.common.quarkus.properties.JsonUtil;
import io.clonecloudstore.common.quarkus.properties.QuarkusProperties;
import io.clonecloudstore.common.standard.system.SystemTools;
import io.clonecloudstore.driver.api.model.StorageObject;
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.CDI;
import org.jboss.logging.Logger;

import static io.clonecloudstore.accessor.server.commons.buffer.CcsBufferService.BUFFERED_IMPORT;

@ApplicationScoped
@Unremovable
public class FilesystemHandler {
  private static final Logger LOGGER = Logger.getLogger(FilesystemHandler.class);
  public static final String X_HASH = "x-hash";
  public static final String X_EXPIRES = "x-expires";
  private static final TypeReference<Map<String, String>> typeReferenceMapStringString = new TypeReference<>() {
  };
  private static final String EXT_METADATA = ".md";
  private static final String EXT_CONTENT = ".bin";
  private static final double GB = 1024.0 * 1024 * 1024;
  private final List<BufferedItem> activeJobs = Collections.synchronizedList(new ArrayList<>());
  private final File root = new File(AccessorProperties.getStorePath());
  private boolean hasDatabase = QuarkusProperties.hasDatabase();
  private final BulkMetrics bulkMetrics;

  protected FilesystemHandler() {
    root.mkdirs(); // NOSONAR intentional
    bulkMetrics = CDI.current().select(BulkMetrics.class).get();
  }

  void changeHasDatabase(final boolean hasDatabase) {
    this.hasDatabase = hasDatabase;
  }

  private String getBaseFilename(final String object) {
    return object.replace('/', '#');
  }

  public synchronized long save(final String bucket, final String object, final InputStream inputStream,
                                final Map<String, String> metadata, final Instant expires) throws IOException {
    if (check(bucket, object)) {
      throw new IOException("Already exist");
    }
    final var filename = getBaseFilename(object);
    final var fileDir = new File(root, bucket);
    fileDir.mkdir();
    final var filebin = new File(fileDir, filename + EXT_CONTENT);
    long size;
    try (final var outputStream = new FileOutputStream(filebin)) {
      size = SystemTools.transferTo(inputStream, outputStream);
      outputStream.flush();
    }
    if (!hasDatabase) {
      final var filemd = new File(fileDir, filename + EXT_METADATA);
      try (final var outputStream = new FileOutputStream(filemd)) {
        var map = new HashMap<String, String>();
        if (metadata != null) {
          map.putAll(metadata);
        }
        if (expires != null) {
          map.put(X_EXPIRES, expires.toString());
        }
        outputStream.write(JsonUtil.getInstance().writeValueAsBytes(map));
        outputStream.flush();
      }
    }
    bulkMetrics.incrementCounter(1, BUFFERED_IMPORT, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_CREATE);
    LOGGER.infof("Save one object into buffer: %s %s", bucket, object);
    return size;
  }

  public synchronized void update(final String bucket, final String object, final Map<String, String> metadata,
                                  final String hash) throws IOException {
    if (hasDatabase) {
      throw new IOException("No Metadata as file");
    }
    if (!check(bucket, object)) {
      throw new FileNotFoundException("Does not exist");
    }
    final var filename = getBaseFilename(object);
    final var fileDir = new File(root, bucket);
    final var filemd = new File(fileDir, filename + EXT_METADATA);
    var map = readMetadata(bucket, object);
    try (final var outputStream = new FileOutputStream(filemd)) {
      if (metadata != null) {
        map.putAll(metadata);
      }
      if (hash != null) {
        map.put(X_HASH, hash);
      }
      outputStream.write(JsonUtil.getInstance().writeValueAsBytes(map));
      outputStream.flush();
    }
    LOGGER.infof("Update one object into buffer: %s %s", bucket, object);
  }

  public InputStream readContent(final String bucket, final String object) throws FileNotFoundException {
    final var filename = getBaseFilename(object);
    final var filebin = new File(new File(root, bucket), filename + EXT_CONTENT);
    if (!filebin.isFile()) {
      throw new FileNotFoundException("Not exist");
    }
    bulkMetrics.incrementCounter(1, BUFFERED_IMPORT, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_READ);
    LOGGER.infof("Read one object into buffer: %s %s", bucket, object);
    return new FileInputStream(filebin);
  }

  protected Map<String, String> readMetadata(final String bucket, final String object) throws IOException {
    if (hasDatabase) {
      throw new IOException("No Metadata as file");
    }
    final var filename = getBaseFilename(object);
    final var filemd = new File(new File(root, bucket), filename + EXT_METADATA);
    if (!filemd.isFile()) {
      throw new FileNotFoundException("Not exist");
    }
    bulkMetrics.incrementCounter(1, BUFFERED_IMPORT, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_READ_MD);
    LOGGER.infof("Read one Metadata object into buffer: %s %s", bucket, object);
    return JsonUtil.getInstance().readValue(filemd, typeReferenceMapStringString);
  }

  public StorageObject readStorageObject(final String bucket, final String object) throws IOException {
    final var map = readMetadata(bucket, object);
    final var hash = map.remove(X_HASH);
    final var expiresString = map.remove(X_EXPIRES);
    Instant expires = null;
    if (expiresString != null) {
      expires = Instant.parse(expiresString);
    }
    final var filename = getBaseFilename(object);
    final var filebin = new File(new File(root, bucket), filename + EXT_CONTENT);
    LOGGER.infof("Read one Storage object into buffer: %s %s", bucket, object);
    return new StorageObject(bucket, object, hash, filebin.length(),
        Files.getLastModifiedTime(filebin.toPath()).toInstant(), expires, map);
  }

  protected boolean delete(final String bucket, final String object) {
    final var filename = getBaseFilename(object);
    final var fileDir = new File(root, bucket);
    final var filebin = new File(fileDir, filename + EXT_CONTENT);
    var deleted = internalDelete(filebin.toPath());
    if (!hasDatabase) {
      final var filemd = new File(fileDir, filename + EXT_METADATA);
      internalDelete(filemd.toPath());
    }
    if (deleted) {
      LOGGER.infof("Delete one object into buffer: %s %s", bucket, object);
    }
    bulkMetrics.incrementCounter(1, BUFFERED_IMPORT, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_DELETE);
    return deleted;
  }

  private boolean internalDelete(final Path path) {
    try {
      Files.delete(path);
      return true;
    } catch (final IOException ignore) {
      // Ignore
    }
    return false;
  }

  public boolean check(final String bucket, final String object) {
    final var filename = getBaseFilename(object);
    final var fileDir = new File(root, bucket);
    final var filebin = new File(fileDir, filename + EXT_CONTENT);
    if (!hasDatabase) {
      final var filemd = new File(fileDir, filename + EXT_METADATA);
      return filebin.isFile() && filemd.isFile();
    }
    return filebin.isFile();
  }

  public synchronized void registerItem(final String bucket, final String object) {
    bulkMetrics.incrementCounter(1, BUFFERED_IMPORT, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_REGISTER);
    activeJobs.add(new BufferedItem(bucket, object));
    LOGGER.infof("Register one object into buffer: %s %s", bucket, object);
  }

  public synchronized boolean unregisterItem(final String bucket, final String object) {
    var removed = activeJobs.remove(new BufferedItem(bucket, object));
    if (removed) {
      bulkMetrics.incrementCounter(1, BUFFERED_IMPORT, BulkMetrics.KEY_OBJECT, BulkMetrics.TAG_UNREGISTER);
      LOGGER.infof("Unregister one object into buffer: %s %s", bucket, object);
    }
    return delete(bucket, object);
  }

  public synchronized List<BufferedItem> getCurrentRegisteredTasks() {
    return new ArrayList<>(activeJobs);
  }

  public synchronized void removedValidatedTasks(final List<BufferedItem> validated) {
    activeJobs.removeAll(validated);
  }

  public void checkFreeSpaceGb(final long size) throws IOException {
    final double freeSpace = root.getFreeSpace();
    if (freeSpace < size || freeSpace / GB < AccessorProperties.getStoreMinSpaceGb()) {
      throw new IOException("Not enough space on device");
    }
  }

  public synchronized long deleteOlderThan(final Instant limitDate) throws IOException {
    final AtomicLong count = new AtomicLong();
    try (final Stream<Path> stream = Files.walk(root.toPath())) {
      stream.filter(path -> {
        try {
          return Files.isRegularFile(path) && Files.getLastModifiedTime(path).toInstant().isBefore(limitDate);
        } catch (final IOException e) {
          return false;
        }
      }).forEach(path -> {
        try {
          Files.delete(path);
          count.incrementAndGet();
        } catch (final IOException ignore) {
          // Ignore
        }
      });
      if (!hasDatabase) {
        return count.get() / 2;
      }
      return count.get();
    }
  }

  public long count() throws IOException {
    try (final Stream<Path> stream = Files.walk(root.toPath())) {
      if (!hasDatabase) {
        return stream.filter(Files::isRegularFile).count() / 2;
      }
      return stream.filter(Files::isRegularFile).count();
    }
  }

  public long size() throws IOException {
    final AtomicLong size = new AtomicLong();
    try (final Stream<Path> stream = Files.walk(root.toPath())) {
      stream.forEach(path -> {
        try {
          if (Files.isRegularFile(path) && !path.toString().endsWith(EXT_METADATA)) {
            size.addAndGet(Files.size(path));
          }
        } catch (final IOException ignore) {
          // Ignore
        }
      });
      return size.get();
    }
  }
}
