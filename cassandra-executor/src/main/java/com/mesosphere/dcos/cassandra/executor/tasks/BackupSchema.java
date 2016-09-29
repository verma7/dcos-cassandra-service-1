package com.mesosphere.dcos.cassandra.executor.tasks;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.mesosphere.dcos.cassandra.common.config.CassandraApplicationConfig;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSchemaStatus;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSchemaTask;
import com.mesosphere.dcos.cassandra.executor.CassandraDaemonProcess;
import com.mesosphere.dcos.cassandra.executor.backup.BackupStorageDriver;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

public class BackupSchema implements Runnable{
    private static final Logger LOGGER = LoggerFactory.getLogger(
            BackupSchema.class);
    private ExecutorDriver driver;
    private CassandraDaemonProcess daemon;
    private final BackupContext context;
    private BackupSchemaTask cassandraTask;
    private final BackupStorageDriver backupStorageDriver;

    private void sendStatus(ExecutorDriver driver,
                            Protos.TaskState state,
                            String message) {
        Protos.TaskStatus status = BackupSchemaStatus.create(
                state,
                cassandraTask.getId(),
                cassandraTask.getSlaveId(),
                cassandraTask.getExecutor().getId(),
                Optional.of(message)
        ).toProto();
        driver.sendStatusUpdate(status);
    }

    /**
     * Constructs a BackupSchema.
     * @param driver The ExecutorDriver used to send task status.
     * @param daemon The CassandraDaemonProcess used to perform the schema.
     * @param cassandraTask The CassandraTask that will be executed by the
     *                      BackupSchema.
     */
    public BackupSchema(ExecutorDriver driver,
                        CassandraDaemonProcess daemon,
                        BackupSchemaTask cassandraTask,
                        String nodeId,
                        BackupStorageDriver backupStorageDriver) {
        this.driver = driver;
        this.daemon = daemon;
        this.cassandraTask = cassandraTask;
        this.backupStorageDriver = backupStorageDriver;
        context = new BackupContext();
        context.setNodeId(nodeId);
        context.setName(this.cassandraTask.getBackupName());
        context.setExternalLocation(this.cassandraTask.getExternalLocation());
        context.setS3AccessKey(this.cassandraTask.getS3AccessKey());
        context.setS3SecretKey(this.cassandraTask.getS3SecretKey());
    }

    @Override
    public void run(){
        Cluster cluster = null;
        try{
            // Send TASK_RUNNING
            sendStatus(driver, Protos.TaskState.TASK_RUNNING,
                    "Started taking schema backup");

            cluster = Cluster.builder().addContactPoint(daemon.getProbe().getEndpoint()).build();
            final List<String> nonSystemKeyspaces = daemon.getNonSystemKeySpaces();

            LOGGER.info("Started taking schema for non system keyspaces: {}", nonSystemKeyspaces);
            StringBuilder sb = new StringBuilder();
            for (String keyspace : nonSystemKeyspaces) {
                if (!CassandraApplicationConfig.SYSTEM_KEYSPACE_LIST.contains(keyspace)) {
                    LOGGER.info("Taking schema for keyspace: {}", keyspace);
                    KeyspaceMetadata ksm = cluster.getMetadata().getKeyspace(keyspace);
                    sb.append(ksm.exportAsString()).append(System.getProperty("line.separator"));
                }
            }
            if (sb.length() > 0) {
                backupStorageDriver.uploadSchema(context, sb.toString());
            }

            // Send TASK_FINISHED
            sendStatus(driver, Protos.TaskState.TASK_FINISHED,
                    "Finished taking schema for non system keyspaces: " + nonSystemKeyspaces);
        } catch (Throwable t){
            LOGGER.error("Schema backup failed. Reason: ", t);
            sendStatus(driver, Protos.TaskState.TASK_FAILED, t.getMessage());
        } finally {
            if (cluster != null)
                cluster.close();
        }
    }
}
