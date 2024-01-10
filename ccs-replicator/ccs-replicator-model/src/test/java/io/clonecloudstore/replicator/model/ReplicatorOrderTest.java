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

package io.clonecloudstore.replicator.model;

import io.clonecloudstore.accessor.config.AccessorConstants;
import io.clonecloudstore.common.standard.guid.GuidLike;
import io.clonecloudstore.replicator.config.ReplicatorConstants;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.clonecloudstore.accessor.config.AccessorConstants.Api.X_CLIENT_ID;
import static io.clonecloudstore.common.standard.properties.ApiConstants.X_OP_ID;
import static io.clonecloudstore.replicator.config.ReplicatorConstants.Action.CREATE;
import static io.clonecloudstore.replicator.config.ReplicatorConstants.Action.DELETE;
import static io.clonecloudstore.replicator.config.ReplicatorConstants.Api.X_TARGET_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class ReplicatorOrderTest {
  @Test
  void checkInit() {
    final var object =
        new ReplicatorOrder(GuidLike.getGuid(), "FROM_SITE", "TO_SITE", "CLIENT_ID", "BUCKET_NAME", "OBJECT_PATH", 0,
            null, CREATE);
    var object2 =
        new ReplicatorOrder(object.opId(), object.fromSite(), object.toSite(), object.clientId(), object.bucketName(),
            object.objectName(), 0, null, object.action());
    assertFalse(object.equals(null));
    assertFalse(object.equals("xx"));
    assertEquals(object, object);
    assertEquals(object, new ReplicatorOrder(object, CREATE));
    assertEquals(object, new ReplicatorOrder(object, "TO_SITE"));
    assertEquals(object, object2);
    assertNotEquals(object, new ReplicatorOrder(object, DELETE));
    assertNotEquals(object, new ReplicatorOrder(object, "TO_SITE2"));

    assertEquals(object.hashCode(), object2.hashCode());

    final var map = object.getHeaders();
    assertEquals(3, map.size());
    assertEquals(object.toSite(), map.get(X_TARGET_ID));
    assertEquals(object.clientId(), map.get(X_CLIENT_ID));
    assertEquals(object.opId(), map.get(X_OP_ID));
    assertEquals(object.toString(), object2.toString());
    assertTrue(object.toString().contains("CLIENT_ID"));

    object2 = new ReplicatorOrder(object.opId(), object.fromSite() + "a", object.toSite(), object.clientId(),
        object.bucketName(), object.objectName(), 0, null, object.action());
    assertNotEquals(object, object2);
    object2 = new ReplicatorOrder(object.opId(), object.fromSite(), object.toSite() + "a", object.clientId(),
        object.bucketName(), object.objectName(), 0, null, object.action());
    assertNotEquals(object, object2);
    object2 = new ReplicatorOrder(object.opId(), object.fromSite(), object.toSite(), object.clientId() + "a",
        object.bucketName(), object.objectName(), 0, null, object.action());
    assertNotEquals(object, object2);
    object2 = new ReplicatorOrder(object.opId(), object.fromSite(), object.toSite(), object.clientId(),
        object.bucketName() + "a", object.objectName(), 0, null, object.action());
    assertNotEquals(object, object2);
    object2 =
        new ReplicatorOrder(object.opId(), object.fromSite(), object.toSite(), object.clientId(), object.bucketName(),
            object.objectName() + "a", 0, null, object.action());
    assertNotEquals(object, object2);
    object2 =
        new ReplicatorOrder(object.opId(), object.fromSite(), object.toSite(), object.clientId(), object.bucketName(),
            object.objectName(), 10, null, object.action());
    assertNotEquals(object, object2);
    object2 =
        new ReplicatorOrder(object.opId(), object.fromSite(), object.toSite(), object.clientId(), object.bucketName(),
            object.objectName(), 0, "sha", object.action());
    assertNotEquals(object, object2);
    object2 =
        new ReplicatorOrder(object.opId(), object.fromSite(), object.toSite(), object.clientId(), object.bucketName(),
            object.objectName(), 0, null, DELETE);
    assertNotEquals(object, object2);
  }

  @Test
  void checkGetHeaders() {
    var object =
        new ReplicatorOrder(GuidLike.getGuid(), "FROM_SITE", "TO_SITE", "CLIENT_ID", "BUCKET_NAME", "OBJECT_PATH", 0,
            null, CREATE);
    var map = object.getHeaders();
    assertEquals(object.clientId(), map.get(AccessorConstants.Api.X_CLIENT_ID));
    assertEquals(object.toSite(), map.get(ReplicatorConstants.Api.X_TARGET_ID));
    assertEquals(object.opId(), map.get(X_OP_ID));
    assertEquals(3, map.size());

    object = new ReplicatorOrder(null, "FROM_SITE", "TO_SITE", null, "BUCKET_NAME", "OBJECT_PATH", 0, null, CREATE);
    map = object.getHeaders();
    assertNull(map.get(X_CLIENT_ID));
    assertEquals(object.toSite(), map.get(ReplicatorConstants.Api.X_TARGET_ID));
    assertEquals(1, map.size());
  }
}
