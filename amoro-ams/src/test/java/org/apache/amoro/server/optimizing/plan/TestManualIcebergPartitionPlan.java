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

package org.apache.amoro.server.optimizing.plan;

import org.apache.amoro.BasicTableTestHelper;
import org.apache.amoro.OptimizerProperties;
import org.apache.amoro.TableFormat;
import org.apache.amoro.TableTestHelper;
import org.apache.amoro.catalog.BasicCatalogTestHelper;
import org.apache.amoro.catalog.CatalogTestHelper;
import org.apache.amoro.optimizing.IcebergRewriteExecutorFactory;
import org.apache.amoro.optimizing.OptimizingType;
import org.apache.amoro.optimizing.RewriteStageTask;
import org.apache.amoro.optimizing.TaskProperties;
import org.apache.amoro.optimizing.plan.AbstractOptimizingPlanner;
import org.apache.amoro.optimizing.plan.AbstractPartitionPlan;
import org.apache.amoro.optimizing.plan.IcebergPartitionPlan;
import org.apache.amoro.optimizing.scan.IcebergTableFileScanHelper;
import org.apache.amoro.optimizing.scan.TableFileScanHelper;
import org.apache.amoro.server.optimizing.OptimizingTestHelpers;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.apache.amoro.server.utils.IcebergTableUtil;
import org.apache.amoro.shade.guava32.com.google.common.collect.Lists;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.amoro.table.TableProperties;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.data.Record;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class TestManualIcebergPartitionPlan extends MixedTablePlanTestBase {

  private OptimizingType forcedType;

  public TestManualIcebergPartitionPlan(
      CatalogTestHelper catalogTestHelper, TableTestHelper tableTestHelper) {
    super(catalogTestHelper, tableTestHelper);
  }

  @Parameterized.Parameters(name = "{0}, {1}")
  public static Object[][] parameters() {
    return new Object[][] {
      {new BasicCatalogTestHelper(TableFormat.ICEBERG), new BasicTableTestHelper(false, false)}
    };
  }

  @Test
  public void testForcedMinorIgnoresFileCountAndSkipsUndersizedSegments() {
    suppressAutomaticTriggers();
    List<DataFile> fragments = writeFiles(1, 4, 5, 8);
    List<DataFile> undersized = writeFiles(9, 40, 41, 80);
    classifyFragmentsAndUndersized(fragments, undersized);

    forcedType = null;
    Assert.assertTrue(planWithCurrentFiles().isEmpty());

    forcedType = OptimizingType.MINOR;
    List<RewriteStageTask> minorTasks = planWithCurrentFiles();
    Assert.assertEquals(1, minorTasks.size());
    assertTask(
        minorTasks.get(0),
        fragments,
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList());
    Assert.assertEquals(OptimizingType.MINOR, buildPlanWithCurrentFiles().getOptimizingType());
  }

  @Test
  public void testForcedMajorMergesUndersizedWithoutEnoughContent() {
    suppressAutomaticTriggers();
    List<DataFile> fragments = writeFiles(1, 4, 5, 8);
    List<DataFile> undersized = writeFiles(9, 40, 41, 80);
    classifyFragmentsAndUndersized(fragments, undersized);
    openFullOptimizing();

    forcedType = OptimizingType.MAJOR;
    List<RewriteStageTask> tasks = planWithCurrentFiles();
    List<DataFile> expected = Lists.newArrayList();
    expected.addAll(fragments);
    expected.addAll(undersized);
    Assert.assertEquals(paths(expected), rewrittenPaths(tasks));
    Assert.assertEquals(OptimizingType.MAJOR, buildPlanWithCurrentFiles().getOptimizingType());
  }

  @Test
  public void testForcedMajorDoesNotRewriteHealthyFiles() {
    suppressAutomaticTriggers();
    List<DataFile> healthy = writeFiles(1, 80, 81, 160);
    markHealthy(healthy);
    openFullOptimizing();

    forcedType = OptimizingType.MAJOR;
    Assert.assertTrue(planWithCurrentFiles().isEmpty());

    forcedType = null;
    Assert.assertFalse(planWithCurrentFiles().isEmpty());
    Assert.assertEquals(OptimizingType.FULL, buildPlanWithCurrentFiles().getOptimizingType());
    closeFullOptimizingInterval();
    Assert.assertTrue(planWithCurrentFiles().isEmpty());
  }

  @Test
  public void testForcedFullIgnoresDisabledInterval() {
    suppressAutomaticTriggers();
    List<DataFile> healthy = writeFiles(1, 80, 81, 160);
    markHealthy(healthy);

    forcedType = null;
    Assert.assertTrue(planWithCurrentFiles().isEmpty());

    forcedType = OptimizingType.FULL;
    List<RewriteStageTask> tasks = planWithCurrentFiles();
    Assert.assertEquals(1, tasks.size());
    assertTask(
        tasks.get(0),
        healthy,
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList());
    Assert.assertEquals(OptimizingType.FULL, buildPlanWithCurrentFiles().getOptimizingType());

    Mockito.when(getTableRuntime().getManualOptimizingType()).thenReturn(OptimizingType.FULL);
    Mockito.when(getTableRuntime().getLastMinorOptimizingTime()).thenReturn(0L);
    Mockito.when(getTableRuntime().getLastMajorOptimizingTime()).thenReturn(0L);
    Mockito.when(getTableRuntime().getLastFullOptimizingTime()).thenReturn(0L);
    AbstractOptimizingPlanner planner =
        IcebergTableUtil.createOptimizingPlanner(
            getTableRuntime(),
            getMixedTable(),
            1,
            OptimizerProperties.MAX_INPUT_FILE_SIZE_PER_THREAD_DEFAULT);
    Assert.assertTrue(planner.isNecessary());
    Assert.assertEquals(OptimizingType.FULL, planner.getOptimizingType());
  }

  private void suppressAutomaticTriggers() {
    updateTableProperty(TableProperties.SELF_OPTIMIZING_MINOR_TRIGGER_INTERVAL, "-1");
    updateTableProperty(TableProperties.SELF_OPTIMIZING_MINOR_TRIGGER_FILE_CNT, "1000");
    closeFullOptimizingInterval();
  }

  private List<DataFile> writeFiles(int from, int to, int from2, int to2) {
    List<DataFile> files = Lists.newArrayList();
    files.addAll(writeRange(from, to));
    files.addAll(writeRange(from2, to2));
    return files;
  }

  private List<DataFile> writeRange(int from, int to) {
    List<Record> records =
        OptimizingTestHelpers.generateRecord(tableTestHelper(), from, to, "2022-01-01T12:00:00");
    long transactionId = beginTransaction();
    return OptimizingTestHelpers.appendBase(
        getMixedTable(),
        tableTestHelper().writeBaseStore(getMixedTable(), transactionId, records, false));
  }

  private void classifyFragmentsAndUndersized(List<DataFile> fragments, List<DataFile> undersized) {
    long smallMax = maxSize(fragments);
    long largeMin = minSize(undersized);
    long largeMax = maxSize(undersized);
    Assert.assertTrue(smallMax < largeMin);
    long ratio = 2;
    while ((long) (0.75 * smallMax * ratio) < largeMax) {
      ratio++;
    }
    long target = smallMax * ratio;
    Assert.assertTrue(target / ratio >= smallMax);
    Assert.assertTrue(target / ratio < largeMin);
    Assert.assertTrue(largeMax <= (long) (target * 0.75));
    updateTableProperty(TableProperties.SELF_OPTIMIZING_TARGET_SIZE, Long.toString(target));
    updateTableProperty(TableProperties.SELF_OPTIMIZING_FRAGMENT_RATIO, Long.toString(ratio));
  }

  private void markHealthy(List<DataFile> files) {
    long fileMin = minSize(files);
    updateTableProperty(TableProperties.SELF_OPTIMIZING_TARGET_SIZE, Long.toString(fileMin));
    updateTableProperty(TableProperties.SELF_OPTIMIZING_FRAGMENT_RATIO, "8");
    Assert.assertTrue(fileMin > fileMin / 8);
    Assert.assertTrue(fileMin > (long) (fileMin * 0.75));
  }

  private static long maxSize(List<DataFile> files) {
    return files.stream().mapToLong(ContentFile::fileSizeInBytes).max().orElseThrow();
  }

  private static long minSize(List<DataFile> files) {
    return files.stream().mapToLong(ContentFile::fileSizeInBytes).min().orElseThrow();
  }

  private static Set<String> paths(List<DataFile> files) {
    return files.stream()
        .map(ContentFile::path)
        .map(CharSequence::toString)
        .collect(Collectors.toSet());
  }

  private static Set<String> rewrittenPaths(List<RewriteStageTask> tasks) {
    return tasks.stream()
        .flatMap(task -> Arrays.stream(task.getInput().rewrittenDataFiles()))
        .map(ContentFile::path)
        .map(CharSequence::toString)
        .collect(Collectors.toSet());
  }

  @Override
  protected AbstractPartitionPlan getPartitionPlan() {
    DefaultTableRuntime tableRuntime = getTableRuntime();
    return new IcebergPartitionPlan(
        tableRuntime.getTableIdentifier(),
        tableRuntime.getOptimizingConfig(),
        getMixedTable(),
        getPartition(),
        System.currentTimeMillis(),
        tableRuntime.getLastMinorOptimizingTime(),
        tableRuntime.getLastFullOptimizingTime(),
        tableRuntime.getLastMajorOptimizingTime(),
        forcedType);
  }

  @Override
  protected TableFileScanHelper getTableFileScanHelper() {
    long baseSnapshotId = IcebergTableUtil.getSnapshotId(getMixedTable().asUnkeyedTable(), true);
    return new IcebergTableFileScanHelper(getMixedTable().asUnkeyedTable(), baseSnapshotId);
  }

  @Override
  protected Map<String, String> buildTaskProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(
        TaskProperties.TASK_EXECUTOR_FACTORY_IMPL, IcebergRewriteExecutorFactory.class.getName());
    return properties;
  }
}
