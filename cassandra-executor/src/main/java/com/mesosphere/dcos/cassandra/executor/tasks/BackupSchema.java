package com.mesosphere.dcos.cassandra.executor.tasks;

import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSchemaStatus;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSchemaTask;
import com.mesosphere.dcos.cassandra.executor.CassandraDaemonProcess;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * Created by varung on 8/19/16.
 */
public class BackupSchema implements Runnable{
    private static final Logger LOGGER = LoggerFactory.getLogger(
            BackupSchema.class);
    private ExecutorDriver driver;
    private CassandraDaemonProcess daemon;
    private BackupSchemaTask cassandraTask;

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
                        BackupSchemaTask cassandraTask) {
        this.driver = driver;
        this.daemon = daemon;
        this.cassandraTask = cassandraTask;
    }

    @Override
    public void run(){

        try{
            // Send TASK_RUNNING
            sendStatus(driver, Protos.TaskState.TASK_RUNNING,
                    "Started taking schema backup");

            //final String schemaName = this.cassandraTask.getBackupName();

            final List<String> nonSystemKeyspaces = daemon
                    .getNonSystemKeySpaces();
            LOGGER.info("Started taking schema for non system keyspaces: {}",
                    nonSystemKeyspaces);

            TTransport tr = new TFramedTransport(new TSocket(daemon.getTask().getHostname(), 9042));

            TProtocol proto = new TBinaryProtocol(tr);
            Cassandra.Client client = new Cassandra.Client(proto);
            tr.open();
            for (String keyspace : nonSystemKeyspaces) {
                LOGGER.info("Taking schema for keyspace: {}", keyspace);
                //daemon.takeSchema(schemaName, keyspace);
                //KsDef k = client.describe_keyspace(keyspace);
                //k.toString();
            }


            // Send TASK_FINISHED
            sendStatus(driver, Protos.TaskState.TASK_FINISHED,
                    "Finished taking schema for non system keyspaces: " + nonSystemKeyspaces);
        }
        catch (Throwable t){
            LOGGER.error("Schema failed",t);
            sendStatus(driver, Protos.TaskState.TASK_FAILED, t.getMessage());
        }
    }

}
