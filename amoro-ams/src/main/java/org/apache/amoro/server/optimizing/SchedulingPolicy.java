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

package org.apache.amoro.server.optimizing;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.resource.ResourceGroup;
import org.apache.amoro.server.optimizing.sorter.QuotaOccupySorter;
import org.apache.amoro.server.optimizing.sorter.SorterFactory;
import org.apache.amoro.server.table.DefaultTableRuntime;
import org.apache.amoro.shade.guava32.com.google.common.annotations.VisibleForTesting;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SchedulingPolicy {

  public static final Logger LOG = LoggerFactory.getLogger(SchedulingPolicy.class);

  private static final String SCHEDULING_POLICY_PROPERTY_NAME = "scheduling-policy";

  private final Map<ServerTableIdentifier, DefaultTableRuntime> tableRuntimeMap = new HashMap<>();
  private volatile String policyName;
  private final Lock tableLock = new ReentrantLock();
  private static final Map<String, SorterFactory> sorterFactoryCache = new ConcurrentHashMap<>();

  public SchedulingPolicy(ResourceGroup group) {
    setTableSorterIfNeeded(group);
  }

  public void setTableSorterIfNeeded(ResourceGroup optimizerGroup) {
    tableLock.lock();
    try {
      policyName =
          Optional.ofNullable(optimizerGroup.getProperties())
              .orElseGet(Maps::newHashMap)
              .getOrDefault(SCHEDULING_POLICY_PROPERTY_NAME, QuotaOccupySorter.IDENTIFIER);
    } finally {
      tableLock.unlock();
    }
  }

  static {
    ServiceLoader<SorterFactory> sorterFactories = ServiceLoader.load(SorterFactory.class);
    Iterator<SorterFactory> iterator = sorterFactories.iterator();
    iterator.forEachRemaining(
        sorterFactory -> {
          String identifier = sorterFactory.getIdentifier();
          sorterFactoryCache.put(identifier, sorterFactory);
          LOG.info(
              "Loaded scheduling policy {} and its corresponding sorter instance {}",
              identifier,
              sorterFactory.getClass().getName());
        });
  }

  public String name() {
    return policyName;
  }

  public DefaultTableRuntime scheduleTable(Set<ServerTableIdentifier> skipSet) {
    tableLock.lock();
    try {
      fillSkipSet(skipSet);
      return tableRuntimeMap.values().stream()
          .filter(tableRuntime -> !skipSet.contains(tableRuntime.getTableIdentifier()))
          .min(createSorterByPolicy())
          .orElse(null);
    } finally {
      tableLock.unlock();
    }
  }

  private Comparator<DefaultTableRuntime> createSorterByPolicy() {
    if (sorterFactoryCache.get(policyName) != null) {
      SorterFactory sorterFactory = sorterFactoryCache.get(policyName);
      return sorterFactory.createComparator();
    } else {
      throw new IllegalArgumentException("Unsupported scheduling policy: " + policyName);
    }
  }

  private void fillSkipSet(Set<ServerTableIdentifier> originalSet) {
    long currentTime = System.currentTimeMillis();
    tableRuntimeMap.values().stream()
        .filter(tableRuntime -> shouldSkip(tableRuntime, currentTime))
        .forEach(tableRuntime -> originalSet.add(tableRuntime.getTableIdentifier()));
  }

  /**
   * A manual pending request is eligible without a snapshot change. Its first attempt also skips
   * the min plan interval. After a failed plan, {@code lastPlanTime} moves past the request time
   * and the interval applies again.
   */
  private boolean shouldSkip(DefaultTableRuntime tableRuntime, long currentTime) {
    if (!isTablePending(tableRuntime)) {
      return true;
    }
    if (isManualFirstAttempt(tableRuntime)) {
      return false;
    }
    return currentTime - tableRuntime.getLastPlanTime()
        < tableRuntime.getOptimizingConfig().getMinPlanInterval();
  }

  private boolean isManualFirstAttempt(DefaultTableRuntime tableRuntime) {
    return tableRuntime.getManualOptimizingType() != null
        && tableRuntime.getManualRequestedAt() >= tableRuntime.getLastPlanTime();
  }

  private boolean isTablePending(DefaultTableRuntime tableRuntime) {
    if (tableRuntime.getOptimizingStatus() != OptimizingStatus.PENDING) {
      return false;
    }
    if (tableRuntime.getManualOptimizingType() != null) {
      return true;
    }
    return tableRuntime.getLastOptimizedSnapshotId() != tableRuntime.getCurrentSnapshotId()
        || tableRuntime.getLastOptimizedChangeSnapshotId()
            != tableRuntime.getCurrentChangeSnapshotId();
  }

  public void addTable(DefaultTableRuntime tableRuntime) {
    tableLock.lock();
    try {
      tableRuntimeMap.put(tableRuntime.getTableIdentifier(), tableRuntime);
    } finally {
      tableLock.unlock();
    }
  }

  public void removeTable(DefaultTableRuntime tableRuntime) {
    tableLock.lock();
    try {
      tableRuntimeMap.remove(tableRuntime.getTableIdentifier());
    } finally {
      tableLock.unlock();
    }
  }

  @VisibleForTesting
  Map<ServerTableIdentifier, DefaultTableRuntime> getTableRuntimeMap() {
    return tableRuntimeMap;
  }
}
