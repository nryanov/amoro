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

package org.apache.amoro.server.dashboard.controller;

import io.javalin.http.Context;
import org.apache.amoro.IcebergActions;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableRuntime;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.config.TableConfiguration;
import org.apache.amoro.exception.BadRequestException;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.process.ProcessService;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.apache.amoro.server.table.TableManager;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;

public class TestTriggerCleanup {

  @Test
  public void testRejectUnknownType() {
    TableController controller = controller(TableFormat.ICEBERG, null, null);
    try {
      controller.triggerCleanup(context("not-a-cleanup"));
      Assert.fail("unknown cleanup type should be rejected");
    } catch (BadRequestException expected) {
      Assert.assertTrue(expected.getMessage().contains("expire-snapshots"));
    }
  }

  @Test
  public void testRejectWhenRuntimeIsNotLoaded() {
    TableController controller = controller(TableFormat.ICEBERG, null, null);
    try {
      controller.triggerCleanup(context("expire-snapshots"));
      Assert.fail("missing runtime should be rejected");
    } catch (BadRequestException expected) {
      Assert.assertTrue(expected.getMessage().contains("not loaded"));
    }
  }

  @Test
  public void testRejectDanglingDeleteOnMixedTable() {
    DefaultTableRuntime runtime = Mockito.mock(DefaultTableRuntime.class);
    TableController controller = controller(TableFormat.MIXED_ICEBERG, runtime, null);
    try {
      controller.triggerCleanup(context("clean-dangling-delete-files"));
      Assert.fail("dangling delete on a mixed table should be rejected");
    } catch (BadRequestException expected) {
      Assert.assertTrue(expected.getMessage().contains("Iceberg"));
    }
  }

  @Test
  public void testRejectWhenActionDisabled() {
    DefaultTableRuntime runtime = Mockito.mock(DefaultTableRuntime.class);
    Mockito.when(runtime.getTableConfiguration())
        .thenReturn(new TableConfiguration().setExpireSnapshotEnabled(false));
    TableController controller = controller(TableFormat.ICEBERG, runtime, null);
    try {
      controller.triggerCleanup(context("expire-snapshots"));
      Assert.fail("disabled cleanup action should be rejected");
    } catch (BadRequestException expected) {
      Assert.assertTrue(expected.getMessage().contains("disabled"));
    }
  }

  @Test
  public void testRejectWhenProcessAlreadyRunning() {
    DefaultTableRuntime runtime = Mockito.mock(DefaultTableRuntime.class);
    Mockito.when(runtime.getTableConfiguration())
        .thenReturn(new TableConfiguration().setExpireSnapshotEnabled(true));
    ProcessService processService = Mockito.mock(ProcessService.class);
    Mockito.when(processService.isActionScheduled(IcebergActions.EXPIRE_SNAPSHOTS))
        .thenReturn(true);
    Mockito.when(processService.hasAliveTableProcess(runtime, IcebergActions.EXPIRE_SNAPSHOTS))
        .thenReturn(true);
    TableController controller = controller(TableFormat.ICEBERG, runtime, processService);
    try {
      controller.triggerCleanup(context("expire-snapshots"));
      Assert.fail("an alive cleanup process should be rejected");
    } catch (BadRequestException expected) {
      Assert.assertTrue(expected.getMessage().contains("already running"));
    }
  }

  private static TableController controller(
      TableFormat format, TableRuntime runtime, ProcessService processService) {
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    Mockito.when(catalogManager.catalogExist("catalog")).thenReturn(true);
    TableManager tableManager = Mockito.mock(TableManager.class);
    ServerTableIdentifier identifier =
        ServerTableIdentifier.of("catalog", "database", "table", format);
    identifier.setId(1L);
    Mockito.when(
            tableManager.getServerTableIdentifier(
                Mockito.any(org.apache.amoro.api.TableIdentifier.class)))
        .thenReturn(identifier);
    Mockito.when(tableManager.getTableRuntime(1L)).thenReturn(runtime);
    return new TableController(
        catalogManager, tableManager, null, new Configurations(), processService);
  }

  @SuppressWarnings("unchecked")
  private static Context context(String type) {
    Context ctx = Mockito.mock(Context.class);
    Mockito.when(ctx.pathParam("catalog")).thenReturn("catalog");
    Mockito.when(ctx.pathParam("db")).thenReturn("database");
    Mockito.when(ctx.pathParam("table")).thenReturn("table");
    Mockito.when(ctx.bodyAsClass(Map.class)).thenReturn(Collections.singletonMap("type", type));
    return ctx;
  }
}
