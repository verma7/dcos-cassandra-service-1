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
package com.mesosphere.dcos.cassandra.executor.backup;

import com.google.common.annotations.VisibleForTesting;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.nio.file.Path;

/**
 * Implements a BackupStorageDriver that provides upload and download
 * functionality to file system.
 */
public class FileStorageDriver implements BackupStorageDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileStorageDriver.class);
    private static final String HOST_VOLUME_PATH_PREFIX = "/var/lib/mesos/volumes/roles";

    @VisibleForTesting
    String getLocalBackupPath(final BackupRestoreContext ctx) throws URISyntaxException {
        URI uri = new URI(ctx.getExternalLocation());
        LOGGER.info("URI: " + uri);

        final StringBuilder localBackupPath = new StringBuilder();
        localBackupPath.append(uri.getPath());
        localBackupPath.append(File.separator);
        localBackupPath.append(ctx.getName());

        return localBackupPath.toString();
    }

    private File[] getNonSystemKeyspaces(final BackupRestoreContext ctx) {
        final File file = new File(ctx.getLocalLocation());
        final File[] directories = file
                .listFiles((current, name) -> new File(current, name).isDirectory() && name.compareTo("system") != 0);
        return directories;
    }

    private static File[] getColumnFamilyDir(final File keyspace) {
        return keyspace.listFiles((current, name) -> new File(current, name).isDirectory());
    }

    private void removeOldBackup(final File dir) throws IOException {
        final File[] files = dir.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                removeOldBackup(file);
            }
            file.delete();
        }
        dir.delete();
    }

    private String getHostVolumePath(final File dir, final String volumeId) throws IOException {
        final File[] files = dir.listFiles();
        for (File file : files) {
            if (file.isDirectory() && file.getName().equals(volumeId)) {
                return file.getAbsolutePath();
            } else if (file.isDirectory()) {
                final String path = getHostVolumePath(file, volumeId);
                if (!path.isEmpty()) {
                    //found persistent volume path
                    return path;
                }
            }
        }
        return "";
    }

    private long getDirSize(final File dir) throws IOException {
        long size = 0;
        final File[] files = dir.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                size += getDirSize(file);
            }
            size += file.length();
        }
        return size;
    }

    @Override
    public void upload(BackupRestoreContext ctx) throws Exception {
        final String backupName = ctx.getName();
        final String localBackupPath = getLocalBackupPath(ctx);
        LOGGER.info("Local backup path: " + localBackupPath);
        LOGGER.info("Persistent volume id: " + ctx.getPersistentVolumeId());

        final File volumePathPrefix = new File(HOST_VOLUME_PATH_PREFIX);
        String hostVolumeFullPath = getHostVolumePath(volumePathPrefix, ctx.getPersistentVolumeId());
        if (hostVolumeFullPath.isEmpty()) {
            final String errorMessage = "Invalid host volume path for backup: " + backupName
                    + ", volume id: " + ctx.getPersistentVolumeId();
            LOGGER.error(errorMessage);
            throw new Exception(errorMessage);
        }

        hostVolumeFullPath += File.separator;
        hostVolumeFullPath += "data";

        try {
            LOGGER.info("HostVolumeFullPath: " + hostVolumeFullPath);

            final File dataDirectory = new File(hostVolumeFullPath);
            final File localBackupDir = new File(localBackupPath);

            if (ctx.getMinFreeSpacePercent() > 0.0) {
                double previousBackupSize = 0;
                if (localBackupDir.exists()) {
                    previousBackupSize = (double) getDirSize(localBackupDir);
                    LOGGER.info("Previous backup size {}", (long) previousBackupSize);
                } else {
                    final boolean status = localBackupDir.mkdirs();
                    if (!status) {
                        final String errorMessage = "Failed creating directory for backup: " + backupName
                                + ", dir path: " + localBackupPath;
                        LOGGER.error(errorMessage);
                        throw new Exception(errorMessage);
                    }
                }
                double totalSpace = (double) localBackupDir.getTotalSpace();
                double usableSpace = (double) localBackupDir.getUsableSpace() + previousBackupSize;

                float availableFreeSpacePercent = ((float) (usableSpace / totalSpace)) * 100;

                LOGGER.info("Total space {}", (long) totalSpace);
                LOGGER.info("Usable space {}", (long) usableSpace);
                LOGGER.info("Free space {}", availableFreeSpacePercent);
                LOGGER.info("Available free Space {}%", Math.round(availableFreeSpacePercent));
                if (availableFreeSpacePercent < ctx.getMinFreeSpacePercent()) {
                    final String errorMessage = "Enough free space not avalilable to take backup on " + localBackupPath;
                    LOGGER.error(errorMessage);
                    throw new Exception(errorMessage);
                }
            }

            /**
             * Remove local backup dir (previous backup) - keep only one copy of
             * backup on file system
             */
            if (localBackupDir.exists()) {
                removeOldBackup(localBackupDir);
            }

            final boolean status = localBackupDir.mkdirs();
            if (!status) {
                final String errorMessage = "Failed creating directory for backup: " + backupName + ", dir path: "
                        + localBackupPath;
                LOGGER.error(errorMessage);
                throw new Exception(errorMessage);
            }

            List<File> keyspaces = Arrays.asList(dataDirectory.listFiles()).stream()
                    .filter(x -> !StorageUtil.SKIP_SYSTEM_KEYSPACES.contains(x.getName())).collect(Collectors.toList());
            if (!ctx.getKeySpaces().isEmpty()) {
                keyspaces = keyspaces.stream().filter(x -> ctx.getKeySpaces().contains(x.getName()))
                        .collect(Collectors.toList());
            }
            LOGGER.info("Uploading snapshots for keyspaces: {}",
                    String.join(", ", keyspaces.stream().map(x -> x.getName()).collect(Collectors.toList())));

            // Ex: data/<keyspace>/<cf>/snapshots/</snapshot-dir>/<files>
            for (File keyspaceDir : keyspaces) {
                if (keyspaceDir.isFile()) {
                    // Skip any files in the data directory.
                    // Only enter keyspace directory.
                    continue;
                }
                LOGGER.info("Entering keyspace: {}", keyspaceDir.getName());
                for (final File cfDir : getColumnFamilyDir(keyspaceDir)) {
                    LOGGER.info("Entering column family dir: {}", cfDir.getName());
                    final File snapshotDir = new File(cfDir, "snapshots");
                    final File backupDir = new File(snapshotDir, backupName);
                    if (!StorageUtil.isValidBackupDir(keyspaceDir, cfDir, snapshotDir, backupDir)) {
                        LOGGER.info("Skipping directory: {}", snapshotDir.getAbsolutePath());
                        continue;
                    }
                    LOGGER.info(
                            "Valid backup directories. KeyspaceDir: {} | ColumnFamilyDir: {} | SnapshotDir: {} | BackupName: {}",
                            keyspaceDir.getAbsolutePath(), cfDir.getAbsolutePath(), snapshotDir.getAbsolutePath(),
                            backupName);

                    final Optional<File> snapshotDirectory = StorageUtil.getValidSnapshotDirectory(snapshotDir,
                            backupName);
                    LOGGER.info("Valid snapshot directory: {}", snapshotDirectory.isPresent());
                    if (snapshotDirectory.isPresent()) {
                        // Upload this directory
                        LOGGER.info("Going to upload directory: {}", snapshotDirectory.get().getAbsolutePath());

                        uploadDirectory(backupName, localBackupPath, keyspaceDir.getName(), cfDir.getName(),
                                snapshotDirectory.get());
                    } else {
                        LOGGER.warn("Snapshots directory: {} doesn't contain the current backup directory: {}",
                                snapshotDir.getName(), backupName);
                    }
                }
            }
            LOGGER.info("Done uploading snapshots for backup: {}", backupName);
        } catch (Exception e) {
            LOGGER.error("Failed uploading snapshots for backup: {}, error: {}", backupName, e);
            throw e;
        }
    }

    private void uploadDirectory(final String backupName, final String localBackupPath, final String keyspaceName,
            final String cfName, final File snapshotDirectory) throws Exception {
        try {
            final String cfPath = localBackupPath + File.separator + keyspaceName + File.separator + cfName
                    + File.separator;
            final File dstDir = new File(cfPath);
            if (!dstDir.isDirectory()) {
                final boolean status = dstDir.mkdirs();
                if (!status) {
                    final String errorMessage = "Failed creating directory for backup: " + backupName + ", dir path: "
                            + cfPath;
                    LOGGER.error(errorMessage);
                    throw new Exception(errorMessage);
                }
            }

            //hard-link SSTables
            for (File sstableFile : snapshotDirectory.listFiles()) {
                final Path existingSSTablePath = Paths.get(sstableFile.getAbsolutePath());
                final Path newSSTablePath = Paths.get(cfPath + File.separator + sstableFile.getName());
                Files.createLink(newSSTablePath, existingSSTablePath);
            }
        } catch (Exception e) {
            LOGGER.error("Error occurred on uploading directory {} : {}", snapshotDirectory.getName(), e);
            throw e;
        }
    }

    @Override
    public void uploadSchema(BackupRestoreContext ctx, String schema) throws Exception {
        final String localBackupPath = getLocalBackupPath(ctx);
        final String schemaFilePath = localBackupPath + File.separator + StorageUtil.SCHEMA_FILE;
        final File schemaFile = new File(schemaFilePath);
        FileWriter schemaWriter = null;

        try {
            schemaFile.createNewFile();
            schemaWriter = new FileWriter(schemaFile);
            schemaWriter.write(schema);
            schemaWriter.flush();
        } catch (Exception e) {
            LOGGER.error("Error occurred on uploading schema {} : {}", schemaFilePath, e);
            throw new Exception(e);

        } finally {
            if (schemaWriter != null) {
                schemaWriter.close();
            }
        }
    }

    @Override
    public void download(BackupRestoreContext ctx) throws Exception {
        final String backupName = ctx.getName();
        final File[] keyspaces = getNonSystemKeyspaces(ctx);
        final String localLocation = ctx.getLocalLocation();
        final String localBackupPath = getLocalBackupPath(ctx);

        try {
            if (Objects.equals(ctx.getRestoreType(), new String("new"))) {
                final File backupDir = new File(localBackupPath);
                if (backupDir.exists() == false) {
                    // No backup found
                    throw new Exception("Backup directory not found for the restore: " + ctx.getName()
                            + ", local directory path: " + localBackupPath);
                }
                final List<File> backupFiles = (List<File>) FileUtils.listFilesAndDirs(backupDir,
                        TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);

                final Map<String, Long> snapshotFileKeys = new HashMap<>();
                for (final File dataFile : backupFiles) {
                    if (dataFile.isFile()) {
                        snapshotFileKeys.put(dataFile.getAbsolutePath(), dataFile.getTotalSpace());
                    }
                }

                LOGGER.info("Snapshot files for this node: {}", snapshotFileKeys.toString());
                for (final String fileKey : snapshotFileKeys.keySet()) {
                    final String downloadPath = localLocation + File.separator + ctx.getName() + File.separator
                            + ctx.getNodeId() + fileKey.substring(fileKey.indexOf(backupName) + backupName.length());

                    LOGGER.info("Download path {}", downloadPath);
                    downloadFile(fileKey, downloadPath);
                }
            } else {
                for (final File keyspace : keyspaces) {
                    for (final File cfDir : getColumnFamilyDir(keyspace)) {
                        final String cfPrefix = cfDir.getName().substring(0, cfDir.getName().indexOf("-"));

                        final Map<String, Long> snapshotFileKeys = listSnapshotFiles(
                                localBackupPath + File.separator + keyspace.getName() + File.separator, cfPrefix);
                        for (final String fileKey : snapshotFileKeys.keySet()) {
                            final String destinationFile = cfDir.getAbsolutePath()
                                    + fileKey.substring(fileKey.lastIndexOf(File.separator));
                            downloadFile(fileKey, destinationFile);
                            LOGGER.info("Keyspace {}, Column Family {}, FileKey {}, destination {}", keyspace, cfPrefix,
                                    fileKey, destinationFile);
                        }
                    }
                }
            }
            LOGGER.info("Done downloading snapshots for backup: {}", backupName);
        } catch (Exception e) {
            LOGGER.error("Failed downloading snapshots for backup: {}, error: {}", backupName, e);
            throw new Exception(e);
        }
    }

    private void downloadFile(final String srcFilePath, final String dstFilePath) throws Exception {
        try {
            final File srcFile = new File(srcFilePath);
            final File snapshotFile = new File(dstFilePath);

            // Only create parent directory once, if it doesn't exist.
            final File parentDir = new File(snapshotFile.getParent());
            if (!parentDir.isDirectory()) {
                final boolean parentDirCreated = parentDir.mkdirs();
                if (!parentDirCreated) {
                    LOGGER.error("Error creating parent directory for file: {}. Skipping to next", dstFilePath);
                    return;
                }
            }

            FileUtils.copyFile(srcFile, snapshotFile);
        } catch (Exception e) {
            LOGGER.error("Error downloading the file {} : {}", dstFilePath, e);
            throw new Exception(e);
        }
    }

    @Override
    public String downloadSchema(BackupRestoreContext ctx) throws Exception {

        final String localBackupPath = getLocalBackupPath(ctx);
        final String schemaFilePath = localBackupPath + File.separator + StorageUtil.SCHEMA_FILE;
        final File schemaFile = new File(schemaFilePath);
        if (schemaFile.exists() == false) {
            // No backup found
            LOGGER.error("Schema file not found for the backup {} : {}", ctx.getName(), schemaFile);
            return "";
        }

        BufferedReader br = null;
        final StringBuilder schema = new StringBuilder();

        try {
            final FileReader schemaReader = new FileReader(schemaFile);
            br = new BufferedReader(schemaReader);
            String line = null;

            while ((line = br.readLine()) != null) {
                schema.append(line);
                schema.append("\n");
            }
        } catch (Exception e) {
            LOGGER.error("Error downloading the schema {} : {}", schemaFilePath, e);
            throw new Exception(e);
        } finally {
            if (br != null) {
                br.close();
            }
        }
        return schema.toString();
    }

    private static Map<String, Long> listSnapshotFiles(final String keyspacePath, final String cfPrefix) {
        final File keyspaceFile = new File(keyspacePath);
        final File[] files = keyspaceFile.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith(cfPrefix);
            }
        });
        final Map<String, Long> snapshotFiles = new HashMap<>();

        if (files != null) {
            for (final File cfFile : files) {
                for (final File dataFiles : cfFile.listFiles()) {
                    snapshotFiles.put(dataFiles.getAbsolutePath(), dataFiles.getTotalSpace());
                }
            }
        }
        return snapshotFiles;
    }
}
