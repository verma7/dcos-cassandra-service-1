package com.mesosphere.dcos.cassandra.executor.tasks;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.mesosphere.dcos.cassandra.common.config.CassandraApplicationConfig;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreSchemaStatus;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreSchemaTask;
import com.mesosphere.dcos.cassandra.executor.CassandraDaemonProcess;
import com.mesosphere.dcos.cassandra.executor.backup.BackupStorageDriver;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;
import java.util.stream.Collectors;

/**
 * Restores the schema first before restoring the data. Here, schema is only restored if it does not exist.
 * Restore schema only works on clean cluster.
 */
public class RestoreSchema implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            RestoreSchema.class);

    private final ExecutorDriver driver;
    private final RestoreContext context;
    private CassandraDaemonProcess daemon;
    private final RestoreSchemaTask cassandraTask;
    private BackupStorageDriver backupStorageDriver;
    private final String version;


    /**
     * Constructs a new RestoreSchema.
     *
     * @param driver        The ExecutorDriver used to send task status.
     * @param cassandraTask The RestoreSchemaTask that will be executed.
     * @param nodeId        The id of the node that will be restored.
     * @param version       The version of Cassandra that will be restored.
     */
    public RestoreSchema(
            ExecutorDriver driver,
            CassandraDaemonProcess daemon,
            RestoreSchemaTask cassandraTask,
            String nodeId,
            String version,
            BackupStorageDriver backupStorageDriver) {
        this.driver = driver;
        this.version = version;
        this.cassandraTask = cassandraTask;
        this.daemon = daemon;
        this.backupStorageDriver = backupStorageDriver;

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
        Cluster cluster = null;
        Session session = null;
        Scanner read = null;
        try {
            // Send TASK_RUNNING
            sendStatus(driver, Protos.TaskState.TASK_RUNNING,
                    "Started restoring schema");
            List<String> nonSystemKeyspaces = daemon.getNonSystemKeySpaces();

            nonSystemKeyspaces = nonSystemKeyspaces.stream()
                                  .filter(keyspace -> !CassandraApplicationConfig.SYSTEM_KEYSPACE_LIST.contains(keyspace))
                                  .collect(Collectors.toList());

            LOGGER.info("Started restoring schema for non system keyspaces: {}", nonSystemKeyspaces);

            if (nonSystemKeyspaces.isEmpty()) {
                backupStorageDriver.downloadSchema(context);
                final String schemaFile = context.getLocalLocation() +
                        File.separator + CassandraApplicationConfig.SCHEMAFILENAME;
                LOGGER.info("Path of schema file: " + schemaFile);
                cluster = Cluster.builder().addContactPoint(daemon.getProbe().getEndpoint()).build();
                session = cluster.connect();
                read = new Scanner(new File(schemaFile));
                read.useDelimiter(";");
                while (read.hasNext()) {
                    try {
                        String cqlStmt = read.next().trim();
                        if (cqlStmt.isEmpty())
                            continue;
                        cqlStmt += ";";
                        session.execute(cqlStmt);
                        LOGGER.info("cql stmt: {}", cqlStmt);
                    } catch (AlreadyExistsException e) {
                        LOGGER.info("Schema already exists: {}", e.toString());
                    }
                }
            }

            // Send TASK_FINISHED
            sendStatus(driver, Protos.TaskState.TASK_FINISHED,
                    "Finished restoring schema");
        } catch (Throwable t) {
            // Send TASK_FAILED
            final String errorMessage = "Failed restoring schema. Reason: " + t;
            LOGGER.error(errorMessage);
            sendStatus(driver, Protos.TaskState.TASK_FAILED, errorMessage);
        } finally {
            if (read != null)
                read.close();
            if (session != null)
                session.close();
            if (cluster != null)
                cluster.close();
        }
    }

    private void sendStatus(ExecutorDriver driver,
                            Protos.TaskState state,
                            String message) {

        Protos.TaskStatus status = RestoreSchemaStatus.create(
                state,
                cassandraTask.getId(),
                cassandraTask.getSlaveId(),
                cassandraTask.getExecutor().getId(),
                Optional.of(message)
        ).toProto();
        driver.sendStatusUpdate(status);
    }
}
