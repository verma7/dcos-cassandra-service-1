/*
 * Copyright 2016 Mesosphere
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mesosphere.dcos.cassandra.executor.tasks;

import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreSnapshotStatus;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreSnapshotTask;
import com.mesosphere.dcos.cassandra.executor.CassandraDaemonProcess;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Optional;

/**
 * Implements RestoreSnapshotTask by invoking the Nodetool Refresh / LoadNewSSTables binary that is
 * packaged with the Cassandra distribution.
 */
public class RestoreSnapshot implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            RestoreSnapshot.class);

    private final ExecutorDriver driver;
    private final RestoreContext context;
    private final RestoreSnapshotTask cassandraTask;
    private final String version;
    private final CassandraDaemonProcess daemon;

    /**
     * Constructs a new RestoreSnapshot.
     *
     * @param driver        The ExecutorDriver used to send task status.
     * @param cassandraTask The RestoreSnapshotTask that will be executed.
     * @param nodeId        The id of the node that will be restored.
     * @param version       The version of Cassandra that will be restored.
     */
    public RestoreSnapshot(
            ExecutorDriver driver,
            RestoreSnapshotTask cassandraTask,
            String nodeId,
            String version,
            CassandraDaemonProcess daemon) {
        this.driver = driver;
        this.version = version;
        this.cassandraTask = cassandraTask;
        this.daemon = daemon;
        this.context = new RestoreContext();
        context.setName(this.cassandraTask.getBackupName());
        context.setNodeId(nodeId);
        context.setS3AccessKey(this.cassandraTask.getS3AccessKey());
        context.setS3SecretKey(this.cassandraTask.getS3SecretKey());
        context.setExternalLocation(this.cassandraTask.getExternalLocation());
        context.setLocalLocation(this.cassandraTask.getLocalLocation());
    }

    @Override
    public void run() {
        try {
            // Send TASK_RUNNING
            sendStatus(driver, Protos.TaskState.TASK_RUNNING, "Started restoring snapshot");
            final String localLocation = context.getLocalLocation();
            final List<String> keyspaces = daemon.getNonSystemKeySpaces();
            for (String keyspace : keyspaces) {
                String keyspaceDirPath = localLocation + File.separator + keyspace;
                File keySpaceDir = new File(keyspaceDirPath);
                File[] cfNames = keySpaceDir.listFiles ();
                for (File cfName : cfNames) {
                    if (cfName.isFile ())
                        continue;
                    String columnFamilyName = cfName.getName().substring(0, cfName.getName().indexOf("-"));
                    daemon.getProbe().loadNewSSTables(keyspace, columnFamilyName);
                    LOGGER.info("Keyspace Name " + keyspace + " Column Family Name " + columnFamilyName);
                }
            }
            final String message = "Finished restoring snapshot";
            LOGGER.info(message);
            sendStatus(driver, Protos.TaskState.TASK_FINISHED, message);
        } catch (Throwable t) {
            // Send TASK_FAILED
            final String errorMessage = "Failed restoring snapshot. Reason: " + t;
            LOGGER.error(errorMessage);
            sendStatus(driver, Protos.TaskState.TASK_FAILED, errorMessage);
        }
    }

    private void sendStatus(ExecutorDriver driver,
                            Protos.TaskState state,
                            String message) {

        Protos.TaskStatus status = RestoreSnapshotStatus.create(
                state,
                cassandraTask.getId(),
                cassandraTask.getSlaveId(),
                cassandraTask.getExecutor().getId(),
                Optional.of(message)
        ).toProto();
        driver.sendStatusUpdate(status);
    }
}
