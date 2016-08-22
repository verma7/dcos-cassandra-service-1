package com.mesosphere.dcos.cassandra.executor.tasks;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreSchemaStatus;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreSchemaTask;
import com.mesosphere.dcos.cassandra.executor.CassandraDaemonProcess;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;

/**
 * Created by varung on 8/29/16.
 */
public class RestoreSchema implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            RestoreSchema.class);

    private final ExecutorDriver driver;
    private final RestoreContext context;
    private CassandraDaemonProcess daemon;
    private final RestoreSchemaTask cassandraTask;
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
            String version) {
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
        LOGGER.info("Restore Schema Executor Process Running");
        try {
            // Send TASK_RUNNING
            sendStatus(driver, Protos.TaskState.TASK_RUNNING,
                    "Started restoring schema");

            final String localLocation = context.getLocalLocation();
            final String keyspaceDirectory = localLocation + File.separator +
                    context.getName() + File.separator + context.getNodeId();
            final String schemaFile = keyspaceDirectory + File.separator + "Schema.cql";
            LOGGER.info("Path of schema file: " + schemaFile);



            Cluster cluster = Cluster.builder().addContactPoint(daemon.getProbe().getEndpoint()).build();
            Session session = cluster.connect();

            // Delete the non-system keyspaces before restoring schema
            final List<String> nonSystemKeyspaces = daemon
                    .getNonSystemKeySpaces();
            for (String keyspace: nonSystemKeyspaces) {
                if (keyspace.startsWith("system_"))
                    continue;
                String dropKeyspaceStmt = String.format("DROP KEYSPACE %s;", keyspace);
                session.execute(dropKeyspaceStmt);
                LOGGER.info("Dropping keyspace: " + keyspace);
            }

            Scanner read = new Scanner(new File(schemaFile));
            read.useDelimiter(";");
            while (read.hasNext()) {
                try {
                    String cqlStmt = read.next().trim();
                    if (cqlStmt.isEmpty())
                        continue;
                    cqlStmt += ";";
                    session.execute(cqlStmt);
                } catch (AlreadyExistsException e) {
                    LOGGER.info(e.toString());
                }
            }
            read.close();
            session.close();
            cluster.close();

            // Send TASK_FINISHED
            sendStatus(driver, Protos.TaskState.TASK_FINISHED,
                    "Finished restoring schema");

        } catch (Throwable t) {
            // Send TASK_FAILED
            final String errorMessage = "Failed restoring schema. Reason: " + t;
            LOGGER.error(errorMessage);
            sendStatus(driver, Protos.TaskState.TASK_FAILED, errorMessage);
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
