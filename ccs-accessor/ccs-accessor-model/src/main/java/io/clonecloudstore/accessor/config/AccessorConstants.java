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

package io.clonecloudstore.accessor.config;

/**
 * Constants for Accessor Bucket and Object
 */
public class AccessorConstants {

  /**
   * API Constants
   */
  public static class Api {
    /**
     * Path
     */
    public static final String API_ROOT = "/cloudclonestore";
    public static final String INTERNAL_ROOT = "/ccs/internal";
    /**
     * Tags
     */
    public static final String TAG_PUBLIC = "Public API / ";
    public static final String TAG_INTERNAL = "Internal API / ";
    public static final String TAG_REPLICATOR = "Replicator API / ";
    public static final String TAG_ADMINISTRATION = "Administration API / ";
    public static final String TAG_BUCKET = "Bucket";
    public static final String TAG_OBJECT = "Directory or Object";
    /**
     * Type of Exists: StorageType as Bucket, Directory or Object
     */
    public static final String X_TYPE = "x-clonecloudstore-type";

    /**
     * Full check on Head for Object and Bucket (til the Object Storage)
     */
    public static final String FULL_CHECK = "fullCheck";
    /**
     * Client ID (temporary)
     */
    public static final String X_CLIENT_ID = "x-clonecloudstore-client-id";

    private Api() {
      // Empty
    }
  }

  /**
   * Bucket Headers
   */
  public static class HeaderBucket {
    public static final String X_BUCKET_ID = "x-clonecloudstore-id";
    public static final String X_BUCKET_SITE = "x-clonecloudstore-site";
    public static final String X_BUCKET_NAME = "x-clonecloudstore-name";
    public static final String X_BUCKET_CREATION = "x-clonecloudstore-creation";
    public static final String X_BUCKET_EXPIRES = "x-clonecloudstore-expires";
    public static final String X_BUCKET_STATUS = "x-clonecloudstore-status";

    private HeaderBucket() {
      // Empty
    }
  }

  /**
   * Object Headers
   */
  public static class HeaderObject {
    public static final String X_OBJECT_ID = "x-clonecloudstore-id";
    public static final String X_OBJECT_SITE = "x-clonecloudstore-site";
    public static final String X_OBJECT_BUCKET = "x-clonecloudstore-bucket";
    public static final String X_OBJECT_NAME = "x-clonecloudstore-name";
    public static final String X_OBJECT_HASH = "x-clonecloudstore-hash";
    public static final String X_OBJECT_STATUS = "x-clonecloudstore-status";
    public static final String X_OBJECT_CREATION = "x-clonecloudstore-creation";
    public static final String X_OBJECT_EXPIRES = "x-clonecloudstore-expires";
    public static final String X_OBJECT_SIZE = "x-clonecloudstore-size";
    public static final String X_OBJECT_METADATA = "x-clonecloudstore-metadata";

    private HeaderObject() {
      // Empty
    }
  }

  /**
   * Filter Object Query
   */
  public static class HeaderFilterObject {
    public static final String FILTER_NAME_PREFIX = "x-clonecloudstore-namePrefix";
    public static final String FILTER_STATUSES = "x-clonecloudstore-statuses";
    public static final String FILTER_CREATION_BEFORE = "x-clonecloudstore-creationBefore";
    public static final String FILTER_CREATION_AFTER = "x-clonecloudstore-creationAfter";
    public static final String FILTER_EXPIRES_BEFORE = "x-clonecloudstore-expiresBefore";
    public static final String FILTER_EXPIRES_AFTER = "x-clonecloudstore-expiresAfter";
    public static final String FILTER_SIZE_LT = "x-clonecloudstore-sizeLT";
    public static final String FILTER_SIZE_GT = "x-clonecloudstore-sizeGT";
    public static final String FILTER_METADATA_EQ = "x-clonecloudstore-metadataEq";

    private HeaderFilterObject() {
      // Empty
    }
  }

  private AccessorConstants() {
    // Empty
  }
}
