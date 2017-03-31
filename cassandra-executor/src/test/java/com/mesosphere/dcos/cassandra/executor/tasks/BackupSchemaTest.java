package com.mesosphere.dcos.cassandra.executor.tasks;

import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSchemaStatus;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupSchemaTask;
import com.mesosphere.dcos.cassandra.executor.CassandraDaemonProcess;
import com.mesosphere.dcos.cassandra.executor.backup.BackupStorageDriver;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Optional;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class BackupSchemaTest {

    @Mock
    private ExecutorDriver executorDriver;

    @Mock
    private CassandraDaemonProcess cassandraDaemonProcess;

    @Mock
    private BackupSchemaTask backupSchemaTask;

    @Mock
    private BackupStorageDriver backupStorageDriver;

    @Mock
    private BackupSchemaStatus backupSchemaStatus;

    private BackupSchema backupSchema;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        final BackupRestoreContext context = BackupRestoreContext.create("", "", "", "", "", "", false, "", Arrays.asList("keyspace1", "keyspace2", "keyspace3"), 0.0f);
        when(backupSchemaTask.getBackupRestoreContext()).thenReturn(context);
        when(backupSchemaTask.createStatus(any(Protos.TaskState.class), any(Optional.class))).thenReturn(backupSchemaStatus);
        backupSchema = new BackupSchema(executorDriver, cassandraDaemonProcess, backupSchemaTask, backupStorageDriver);
    }

    @Test
    public void backupSpecificKeySpaces() {
        Assert.assertEquals(backupSchema.getKeySpaces(), Arrays.asList("keyspace1", "keyspace2", "keyspace3"));
    }
}
