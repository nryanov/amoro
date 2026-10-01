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

package org.apache.amoro.server.table.cleanup;

import java.util.LinkedHashSet;
import java.util.Set;

public class TableRuntimeCleanupState {
  private long lastOrphanFilesCleanTime;
  private long lastDanglingDeleteFilesCleanTime;
  private long lastDataExpiringTime;
  private long lastSnapshotsExpiringTime;
  private Set<String> manualActions = new LinkedHashSet<>();
  private long manualRequestedAt;

  public long getLastOrphanFilesCleanTime() {
    return lastOrphanFilesCleanTime;
  }

  public TableRuntimeCleanupState setLastOrphanFilesCleanTime(long lastOrphanFilesCleanTime) {
    this.lastOrphanFilesCleanTime = lastOrphanFilesCleanTime;
    return this;
  }

  public long getLastDanglingDeleteFilesCleanTime() {
    return lastDanglingDeleteFilesCleanTime;
  }

  public TableRuntimeCleanupState setLastDanglingDeleteFilesCleanTime(
      long lastDanglingDeleteFilesCleanTime) {
    this.lastDanglingDeleteFilesCleanTime = lastDanglingDeleteFilesCleanTime;
    return this;
  }

  public long getLastDataExpiringTime() {
    return lastDataExpiringTime;
  }

  public TableRuntimeCleanupState setLastDataExpiringTime(long lastDataExpiringTime) {
    this.lastDataExpiringTime = lastDataExpiringTime;
    return this;
  }

  public long getLastSnapshotsExpiringTime() {
    return lastSnapshotsExpiringTime;
  }

  public TableRuntimeCleanupState setLastSnapshotsExpiringTime(long lastSnapshotsExpiringTime) {
    this.lastSnapshotsExpiringTime = lastSnapshotsExpiringTime;
    return this;
  }

  public Set<String> getManualActions() {
    if (manualActions == null) {
      manualActions = new LinkedHashSet<>();
    }
    return manualActions;
  }

  public void setManualActions(Set<String> manualActions) {
    this.manualActions = new LinkedHashSet<>();
    if (manualActions != null) {
      this.manualActions.addAll(manualActions);
    }
  }

  public long getManualRequestedAt() {
    return manualRequestedAt;
  }

  public void setManualRequestedAt(long manualRequestedAt) {
    this.manualRequestedAt = manualRequestedAt;
  }

  public boolean hasManualAction(String actionName) {
    return actionName != null && getManualActions().contains(actionName);
  }

  /** Adds {@code actionName} once. A second add keeps the original request time. */
  public TableRuntimeCleanupState addManualAction(String actionName, long requestedAt) {
    if (getManualActions().add(actionName)) {
      this.manualRequestedAt = requestedAt;
    }
    return this;
  }

  public TableRuntimeCleanupState clearManualAction(String actionName) {
    if (manualActions != null && manualActions.remove(actionName) && manualActions.isEmpty()) {
      this.manualRequestedAt = 0L;
    }
    return this;
  }
}
