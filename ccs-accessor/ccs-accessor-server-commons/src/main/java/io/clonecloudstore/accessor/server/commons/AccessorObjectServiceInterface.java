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

package io.clonecloudstore.accessor.server.commons;

import java.io.InputStream;

import io.clonecloudstore.accessor.model.AccessorFilter;
import io.clonecloudstore.accessor.model.AccessorObject;
import io.clonecloudstore.common.quarkus.exception.CcsAlreadyExistException;
import io.clonecloudstore.common.quarkus.exception.CcsDeletedException;
import io.clonecloudstore.common.quarkus.exception.CcsNotAcceptableException;
import io.clonecloudstore.common.quarkus.exception.CcsNotExistException;
import io.clonecloudstore.common.quarkus.exception.CcsOperationException;
import io.clonecloudstore.driver.api.StorageType;

public interface AccessorObjectServiceInterface {
  /**
   * Check if object or directory exists (internal)
   *
   * @param bucketName            technical name
   * @param objectOrDirectoryName prefix or full name
   * @param fullCheck             if True, and if Object, will check on Driver Storage
   * @return the associated StorageType
   */
  StorageType objectOrDirectoryExists(final String bucketName, final String objectOrDirectoryName,
                                      final boolean fullCheck) throws CcsOperationException;

  /**
   * Check if object or directory exists
   */
  StorageType objectOrDirectoryExists(final String bucketName, final String objectOrDirectoryName,
                                      final boolean fullCheck, final String clientId, final String opId,
                                      final boolean external) throws CcsOperationException;

  /**
   * @param bucketName technical name
   * @param filter     the filter to apply on Objects
   * @return a stream (InputStream) of AccessorObject line by line (newline separated)
   */
  InputStream filterObjects(final String bucketName, final AccessorFilter filter) throws CcsOperationException;

  /**
   * Get DB Object DTO
   */
  AccessorObject getObjectInfo(final String bucketName, final String objectName)
      throws CcsNotExistException, CcsOperationException;

  /**
   * Before really creating Object, creates it in DB if possible
   *
   * @throws CcsNotAcceptableException if already in creation step
   */
  AccessorObject createObject(final AccessorObject accessorObject, final String hash, final long len)
      throws CcsOperationException, CcsAlreadyExistException, CcsNotExistException, CcsNotAcceptableException;

  /**
   * Once Object really created in Driver Storage, finalize the Object in DB and Replicator if needed
   */
  AccessorObject createObjectFinalize(final AccessorObject accessorObject, final String hash, final long len,
                                      final String clientId, final boolean external);

  /**
   * Delete object in DB and through Replicator if needed
   */
  void deleteObject(final String bucketName, final String objectName, final String clientId, final boolean external)
      throws CcsDeletedException, CcsNotExistException, CcsOperationException;
}
