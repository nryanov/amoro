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

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.exception.BadRequestException;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.optimizing.TableRuntimeOptimizingState;
import org.apache.amoro.server.AMSManagerTestBase;
import org.apache.amoro.server.optimizing.OptimizingProcess;
import org.apache.amoro.server.optimizing.OptimizingStatus;
import org.apache.amoro.server.optimizing.SchedulingPolicy;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.TableRuntimeMeta;
import org.apache.amoro.table.StateKey;
import org.apache.amoro.table.TableProperties;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashSet;

public class TestManualOptimizingRequest extends AMSManagerTestBase {

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
  public void testRequestReplacePendingInputAndClearOnPlan() {
    runtime.requestManualOptimizing(OptimizingType.FULL);
    Assert.assertEquals(OptimizingStatus.PENDING, runtime.getOptimizingStatus());
    Assert.assertEquals(OptimizingType.FULL, runtime.getManualOptimizingType());
    Assert.assertTrue(runtime.getManualRequestedAt() > 0);
    Assert.assertTrue(runtime.getPendingInput().getPartitions().isEmpty());

    runtime.planFailed();
    Assert.assertEquals(OptimizingStatus.PENDING, runtime.getOptimizingStatus());
    Assert.assertEquals(OptimizingType.FULL, runtime.getManualOptimizingType());

    OptimizingProcess process = Mockito.mock(OptimizingProcess.class);
    Mockito.when(process.getProcessId()).thenReturn(42L);
    Mockito.when(process.getOptimizingType()).thenReturn(OptimizingType.FULL);
    runtime.beginProcess(process);
    Assert.assertNull(runtime.getManualOptimizingType());
    Assert.assertEquals(0L, runtime.getManualRequestedAt());
  }

  @Test
  public void testEmptyPlanAndDisableClearRequest() {
    runtime.requestManualOptimizing(OptimizingType.MINOR);
    runtime.completeEmptyProcess();
    Assert.assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
    Assert.assertNull(runtime.getManualOptimizingType());

    runtime.requestManualOptimizing(OptimizingType.MAJOR);
    runtime.clearManualOptimizingRequest();
    Assert.assertEquals(OptimizingStatus.IDLE, runtime.getOptimizingStatus());
    Assert.assertNull(runtime.getManualOptimizingType());
  }

  @Test
  public void testRejectWhenDisabledOrBusy() {
    runtime
        .store()
        .begin()
        .updateTableConfig(
            config -> config.put(TableProperties.ENABLE_SELF_OPTIMIZING, Boolean.FALSE.toString()))
        .commit();
    try {
      runtime.requestManualOptimizing(OptimizingType.MINOR);
      Assert.fail("disabled optimizing should be rejected");
    } catch (BadRequestException expected) {
      Assert.assertTrue(expected.getMessage().contains("disabled"));
    }

    runtime
        .store()
        .begin()
        .updateTableConfig(config -> config.remove(TableProperties.ENABLE_SELF_OPTIMIZING))
        .updateStatusCode(code -> OptimizingStatus.PLANNING.getCode())
        .commit();
    try {
      runtime.requestManualOptimizing(OptimizingType.MAJOR);
      Assert.fail("planning table should be rejected");
    } catch (BadRequestException expected) {
      Assert.assertTrue(expected.getMessage().contains("planning"));
    }
  }

  @Test
  public void testManualRequestSchedulingEligibility() {
    runtime.setLastPlanTime(System.currentTimeMillis());
    runtime.requestManualOptimizing(OptimizingType.MINOR);
    SchedulingPolicy policy = new SchedulingPolicy(defaultResourceGroup());
    policy.addTable(runtime);

    Assert.assertSame(runtime, policy.scheduleTable(new HashSet<>()));

    long minInterval = runtime.getOptimizingConfig().getMinPlanInterval();
    long requestedAt = System.currentTimeMillis() - minInterval - 5_000;
    runtime
        .store()
        .begin()
        .updateState(
            optimizingStateKey(),
            state -> {
              state.setManualRequestedAt(requestedAt);
              return state;
            })
        .commit();
    runtime.setLastPlanTime(System.currentTimeMillis());
    Assert.assertNull(policy.scheduleTable(new HashSet<>()));

    runtime.setLastPlanTime(requestedAt + 1_000);
    Assert.assertSame(runtime, policy.scheduleTable(new HashSet<>()));

    runtime.clearManualOptimizingRequest();
    runtime.store().begin().updateStatusCode(code -> OptimizingStatus.PENDING.getCode()).commit();
    Assert.assertNull(policy.scheduleTable(new HashSet<>()));
  }

  @SuppressWarnings("unchecked")
  private static StateKey<TableRuntimeOptimizingState> optimizingStateKey() {
    return (StateKey<TableRuntimeOptimizingState>)
        DefaultTableRuntime.REQUIRED_STATES.stream()
            .filter(key -> "optimizing_state".equals(key.getKey()))
            .findFirst()
            .orElseThrow(IllegalStateException::new);
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
