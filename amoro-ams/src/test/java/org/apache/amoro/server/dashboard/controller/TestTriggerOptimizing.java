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
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.exception.BadRequestException;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.table.TableManager;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;

public class TestTriggerOptimizing {

  @Test
  public void testRejectNonIceberg() {
    TableController controller = controller(TableFormat.MIXED_ICEBERG, null);
    try {
      controller.triggerOptimizing(context());
      Assert.fail("non-iceberg table should be rejected");
    } catch (BadRequestException expected) {
      Assert.assertTrue(expected.getMessage().contains("Iceberg"));
    }
  }

  @Test
  public void testRejectWhenRuntimeIsNotLoaded() {
    TableController controller = controller(TableFormat.ICEBERG, null);
    try {
      controller.triggerOptimizing(context());
      Assert.fail("missing runtime should be rejected");
    } catch (BadRequestException expected) {
      Assert.assertTrue(expected.getMessage().contains("not loaded"));
    }
  }

  private static TableController controller(TableFormat format, Object runtime) {
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
    Mockito.when(tableManager.getTableRuntime(1L))
        .thenReturn((org.apache.amoro.TableRuntime) runtime);
    return new TableController(catalogManager, tableManager, null, new Configurations());
  }

  @SuppressWarnings("unchecked")
  private static Context context() {
    Context ctx = Mockito.mock(Context.class);
    Mockito.when(ctx.pathParam("catalog")).thenReturn("catalog");
    Mockito.when(ctx.pathParam("db")).thenReturn("database");
    Mockito.when(ctx.pathParam("table")).thenReturn("table");
    Mockito.when(ctx.bodyAsClass(Map.class)).thenReturn(Collections.singletonMap("type", "FULL"));
    return ctx;
  }
}
