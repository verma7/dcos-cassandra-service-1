package com.mesosphere.dcos.cassandra.executor.backup;

import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Collections;

/**
 * This class tests the FileStorageDriver class.
 */
public class FileStorageDriverTest {
    private FileStorageDriver fileStorageDriver;

    @Before
    public void beforeEach() {
        fileStorageDriver = new FileStorageDriver();
    }

    @Test
    public void testGetLocalBkupPathShort() throws URISyntaxException {
        final String localPath = "/tmp/cassandraBackup";
        final BackupRestoreContext backupRestoreContext = BackupRestoreContext.create("node-id", "name",
                "file://" + localPath, "local-location", "", "", false, "existing", Collections.emptyList(), 0.0f, "", "", "");

        Assert.assertEquals(localPath + File.separator + backupRestoreContext.getName(),
                fileStorageDriver.getLocalBackupPath(backupRestoreContext));
    }

    @Test
    public void testGetLocalBkupLong() throws URISyntaxException {
        final String localPath = "/tmp/cassandraBackup/a/b/c/d/e/f";
        final BackupRestoreContext backupRestoreContext = BackupRestoreContext.create("node-id", "name",
                "file://" + localPath, "local-location", "", "", false, "existing", Collections.emptyList(), 0.0f, "", "", "");

        Assert.assertEquals(localPath + File.separator + backupRestoreContext.getName(),
                fileStorageDriver.getLocalBackupPath(backupRestoreContext));
    }
}
