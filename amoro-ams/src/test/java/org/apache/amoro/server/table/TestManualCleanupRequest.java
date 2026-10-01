/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.amoro.server.table;

import org.apache.amoro.IcebergActions;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.exception.BadRequestException;
import org.apache.amoro.server.AMSManagerTestBase;
import org.apache.amoro.server.optimizing.OptimizingStatus;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.TableRuntimeMeta;
import org.apache.amoro.server.table.cleanup.TableRuntimeCleanupState;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

public class TestManualCleanupRequest extends AMSManagerTestBase {

  private Persistence persistence;
  private ServerTableIdentifier tableIdentifier;
  private DefaultTableRuntime runtime;

  @Before
  public void before() {
    persistence = new Persistence();
    tableIdentifier = ServerTableIdentifier.of("catalog", "database", "table", TableFormat.ICEBERG);
    TableRuntimeMeta meta = new TableRuntimeMeta();
    meta.setGroupName("default");
    meta.setStatusCode(OptimizingStatus.IDLE.getCode());
    persistence.persistTableRuntimeMeta(tableIdentifier, meta);
    runtime =
        new DefaultTableRuntime(
            new DefaultTableRuntimeStore(
                tableIdentifier,
                meta,
                DefaultTableRuntime.REQUIRED_STATES,
                Collections.emptyList()),
            () -> null);
  }

  @After
  public void after() {
    persistence.clean(tableIdentifier.getId());
  }

  @Test
  public void testRequestIsIdempotentAndClearRemovesOnlyThatAction() {
    runtime.requestManualCleanup(IcebergActions.EXPIRE_SNAPSHOTS);
    long requestedAt = runtime.getManualCleanupRequestedAt();
    Assert.assertTrue(requestedAt > 0);
    Assert.assertTrue(runtime.hasManualCleanup(IcebergActions.EXPIRE_SNAPSHOTS));

    runtime.requestManualCleanup(IcebergActions.EXPIRE_SNAPSHOTS);
    Assert.assertEquals(requestedAt, runtime.getManualCleanupRequestedAt());

    runtime.requestManualCleanup(IcebergActions.CLEAN_ORPHAN);
    runtime.clearManualCleanup(IcebergActions.EXPIRE_SNAPSHOTS);
    Assert.assertFalse(runtime.hasManualCleanup(IcebergActions.EXPIRE_SNAPSHOTS));
    Assert.assertTrue(runtime.hasManualCleanup(IcebergActions.CLEAN_ORPHAN));
    Assert.assertTrue(runtime.getManualCleanupRequestedAt() > 0);
  }

  @Test
  public void testRejectUnknownAction() {
    try {
      runtime.requestManualCleanup(IcebergActions.REWRITE);
      Assert.fail("rewrite is not a cleanup action");
    } catch (BadRequestException expected) {
      Assert.assertTrue(expected.getMessage().contains("expire-snapshots"));
    }
  }

  @Test
  public void testLegacyCleanupStateHasNoManualAction() {
    String legacy =
        "{\"lastOrphanFilesCleanTime\":1,\"lastDanglingDeleteFilesCleanTime\":2,"
            + "\"lastDataExpiringTime\":3,\"lastSnapshotsExpiringTime\":4}";
    TableRuntimeCleanupState state = DefaultTableRuntime.CLEANUP_STATE_KEY.deserialize(legacy);
    Assert.assertEquals(4L, state.getLastSnapshotsExpiringTime());
    Assert.assertFalse(state.hasManualAction(IcebergActions.EXPIRE_SNAPSHOTS.getName()));
  }

  private static final class Persistence extends PersistentBase {

    void persistTableRuntimeMeta(ServerTableIdentifier identifier, TableRuntimeMeta meta) {
      doAs(
          org.apache.amoro.server.persistence.mapper.TableMetaMapper.class,
          mapper -> mapper.insertTable(identifier));
      meta.setTableId(identifier.getId());
      doAs(
          org.apache.amoro.server.persistence.mapper.TableRuntimeMapper.class,
          mapper -> mapper.insertRuntime(meta));
    }

    void clean(long tableId) {
      doAs(
          org.apache.amoro.server.persistence.mapper.TableRuntimeMapper.class,
          mapper -> mapper.removeAllTableStates(tableId));
      doAs(
          org.apache.amoro.server.persistence.mapper.TableMetaMapper.class,
          mapper -> mapper.deleteTableIdById(tableId));
      doAs(
          org.apache.amoro.server.persistence.mapper.TableRuntimeMapper.class,
          mapper -> mapper.deleteRuntime(tableId));
    }
  }
}
