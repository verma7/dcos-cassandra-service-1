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

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.mesosphere.dcos.cassandra.common.config.CassandraApplicationConfig;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Implements a BackupStorageDriver that provides upload and download
 * functionality to an S3 bucket.
 */
public class S3StorageDriver implements BackupStorageDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            S3StorageDriver.class);

    protected final Set<String> SKIP_KEYSPACES = ImmutableSet.of("system");
    protected final Map<String, List<String>> SKIP_COLUMN_FAMILIES = ImmutableMap.of();

    @Override
    public void upload(BackupContext ctx) throws IOException {
        final String accessKey = ctx.getS3AccessKey();
        final String secretKey = ctx.getS3SecretKey();
        final String localLocation = ctx.getLocalLocation();
        final String backupName = ctx.getName();
        final String nodeId = ctx.getNodeId();

        final AmazonS3URI backupLocationURI = new AmazonS3URI(ctx.getExternalLocation());
        final String bucketName = backupLocationURI.getBucket();

        String prefixKey = backupLocationURI.getKey() != null ? backupLocationURI.getKey() : "";
        prefixKey = (prefixKey.length() > 0 && !prefixKey.endsWith("/")) ? prefixKey + "/" : prefixKey;
        prefixKey += backupName;
        final String key = prefixKey + "/" + nodeId;

        final BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(accessKey, secretKey);
        TransferManager tx = new TransferManager(basicAWSCredentials);

        final File dataDirectory = new File(localLocation);
        // Ex: data/<keyspace>/<cf>/snapshots/</snapshot-dir>/<files>
        for (File keyspaceDir : dataDirectory.listFiles()) {
            if (keyspaceDir.isFile()) {
                // Skip any files in the data directory.
                // Only enter keyspace directory.
                continue;
            }
            LOGGER.info("Entering keyspace: {}", keyspaceDir.getName());
            for (File cfDir : keyspaceDir.listFiles()) {
                LOGGER.info("Entering column family: {}", cfDir.getName());
                File snapshotDir = new File(cfDir, "snapshots");
                if (!isValidBackupDir(keyspaceDir, cfDir, snapshotDir)) {
                    LOGGER.info("Skipping directory: {}", snapshotDir.getAbsolutePath());
                    continue;
                }
                LOGGER.info(
                        "Valid backup directories. Keyspace: {} | ColumnFamily: {} | Snapshot: {} | BackupName: {}",
                        keyspaceDir.getAbsolutePath(), cfDir.getAbsolutePath(),
                        snapshotDir.getAbsolutePath(), backupName);

                final Optional<File> snapshotDirectory = getValidSnapshotDirectory(
                        cfDir, snapshotDir, backupName);
                LOGGER.info("Valid snapshot directory: {}",
                        snapshotDirectory.isPresent());
                if (snapshotDirectory.isPresent()) {
                    // Upload this directory
                    LOGGER.info("Going to upload directory: {}",
                            snapshotDirectory.get().getAbsolutePath());

                    uploadDirectory(tx, bucketName, key, keyspaceDir.getName(), cfDir.getName(), snapshotDirectory.get());
                } else {
                    LOGGER.warn("Snapshots directory: {} doesn't contain the current backup directory: {}",
                            snapshotDir.getName(), backupName);
                }
            }
        }
        tx.shutdownNow();
        LOGGER.info("Done uploading snapshots for backup: {}", backupName);
    }

    private void uploadDirectory(TransferManager tx,
                                 String bucketName,
                                 String key,
                                 String keyspaceName,
                                 String cfName,
                                 File snapshotDirectory) {
        try {
            String fileKey = key + "/" + keyspaceName + "/" + cfName + "/";
            MultipleFileUpload myUpload = tx.uploadDirectory(bucketName, fileKey, snapshotDirectory, true);
            myUpload.waitForCompletion();
        } catch (Exception t) {
            LOGGER.error("Error occurred on uploading {}", t);
        }

    }

    @Override
    public void uploadSchema(BackupContext ctx, String keyspacesSchema) throws IOException{
        final String accessKey = ctx.getS3AccessKey();
        final String secretKey = ctx.getS3SecretKey();
        final String backupName = ctx.getName();
        final String nodeId = ctx.getNodeId();
        InputStream stream = new ByteArrayInputStream(keyspacesSchema.getBytes(StandardCharsets.UTF_8));

        final AmazonS3URI backupLocationURI = new AmazonS3URI(ctx.getExternalLocation());
        final String bucketName = backupLocationURI.getBucket();
        final BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(accessKey, secretKey);
        TransferManager tx = new TransferManager(basicAWSCredentials);

        String prefixKey = backupLocationURI.getKey() != null ? backupLocationURI.getKey() : "";
        prefixKey = (prefixKey.length() > 0 && !prefixKey.endsWith("/")) ? prefixKey + "/" : prefixKey;
        prefixKey += backupName;
        final String key = prefixKey + File.separator + nodeId;
        final String fileKey = key + File.separator + CassandraApplicationConfig.SCHEMAFILENAME;
        LOGGER.info("key {}, filekey {}", key, fileKey);

        uploadFile(tx, bucketName, fileKey, stream);
        tx.shutdownNow();
    }

    private void uploadFile(TransferManager tx,
                            String bucketName,
                            String sourcePrefixKey,
                            InputStream stream) {
        try {
            Upload upload = tx.upload(bucketName, sourcePrefixKey, stream, new ObjectMetadata());
            upload.waitForCompletion();
        } catch (Exception e) {
            LOGGER.info("Uploading File Failed: " + e);
        }
    }

    @Override
    public void downloadSchema(RestoreContext ctx) throws IOException {
        final String accessKey = ctx.getS3AccessKey();
        final String secretKey = ctx.getS3SecretKey();
        // Location of data directory, where the data will be copied.
        final String localLocation = ctx.getLocalLocation();
        final String backupName = ctx.getName();
        final String nodeId = ctx.getNodeId();
        final AmazonS3URI backupLocationURI = new AmazonS3URI(ctx.getExternalLocation());
        final String bucketName = backupLocationURI.getBucket();
        final BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(accessKey, secretKey);
        final TransferManager tx = new TransferManager(basicAWSCredentials);

        final String schemaFilePath = localLocation + File.separator + CassandraApplicationConfig.SCHEMAFILENAME;
        final File schemaFile = new File(schemaFilePath);
        schemaFile.getParentFile().mkdirs();
        final String sourcePrefixKey = backupName + File.separator + nodeId +
                File.separator + CassandraApplicationConfig.SCHEMAFILENAME;
        LOGGER.info(schemaFilePath);
        LOGGER.info("source prefix: {}", sourcePrefixKey);
        downloadFile(tx, bucketName, sourcePrefixKey, schemaFilePath);
        tx.shutdownNow();
    }

    /**
     * Filters unwanted keyspaces and column families
     */
    public boolean isValidBackupDir(File ksDir, File cfDir, File bkDir) {
        if (!bkDir.isDirectory() && !bkDir.exists())
            return false;

        String ksName = ksDir.getName();
        if (SKIP_KEYSPACES.contains(ksName))
            return false;

        String cfName = cfDir.getName();
        if (SKIP_COLUMN_FAMILIES.containsKey(ksName)
                && SKIP_COLUMN_FAMILIES.get(ksName).contains(cfName))
            return false;

        return true;
    }

    private Optional<File> getValidSnapshotDirectory(File cfDir,
                                                     File snapshotsDir,
                                                     String snapshotName) {
        File validSnapshot = null;
        for (File snapshotDir : snapshotsDir.listFiles())
            if (snapshotDir.getName().matches(snapshotName)) {
                // Found requested snapshot directory
                validSnapshot = snapshotDir;
                break;
            }

        // Requested snapshot directory not found
        return Optional.ofNullable(validSnapshot);
    }

    @Override
    public void download(RestoreContext ctx) throws IOException {
        // Ex: data/<keyspace>/<cf>/snapshots/</snapshot-dir>/<files>
        final String accessKey = ctx.getS3AccessKey();
        final String secretKey = ctx.getS3SecretKey();
        // Location of data directory, where the data will be copied.
        final String localLocation = ctx.getLocalLocation();
        final String backupName = ctx.getName();
        final String nodeId = ctx.getNodeId();
        final List<String> keyspaces = ctx.getKeyspaces();

        final AmazonS3URI backupLocationURI = new AmazonS3URI(ctx.getExternalLocation());
        final String bucketName = backupLocationURI.getBucket();

        final BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(accessKey, secretKey);
        final AmazonS3Client amazonS3Client = new AmazonS3Client(basicAWSCredentials);
        final TransferManager tx = new TransferManager(basicAWSCredentials);

        for (String keyspace : keyspaces) {

            String keyspaceDirPath = localLocation + "/" + keyspace;
            LOGGER.info("keyspace path" + keyspaceDirPath);
            File keySpaceDir = new File(keyspaceDirPath);
            File[] cfNames = keySpaceDir.listFiles();

            for (File cfName : cfNames) {
                if (cfName.isFile ())
                    continue;
                String columnFamilyName = cfName.getName().substring(0, cfName.getName().indexOf("-"));
                final Map<String, Long> snapshotFileKeys = listSnapshotFiles(amazonS3Client, bucketName, backupName + "/" + nodeId + "/" + keyspace + "/" + columnFamilyName);
                for(String fileKey: snapshotFileKeys.keySet()) {
                    String destinationDirPath = cfName.getAbsolutePath() + fileKey.substring(fileKey.lastIndexOf("/"));
                    downloadFile(tx, bucketName, fileKey, destinationDirPath);
                    LOGGER.info("Keyspace {}, Column Family {}, FileKey {}, destination {}", keyspace, columnFamilyName, fileKey, destinationDirPath);
                }
            }
        }
        tx.shutdownNow();
    }

    private void downloadFile(TransferManager tx,
                              String bucketName,
                              String sourcePrefixKey,
                              String destinationFile) {
        try {
            File f = new File(destinationFile);
            f.createNewFile();
            Download download = tx.download(bucketName, sourcePrefixKey, f);
            download.waitForCompletion();
        } catch (Exception e) {
            LOGGER.info("Downloading File Failed: " + e);
        }
    }

    private static Map<String, Long> listSnapshotFiles(AmazonS3Client amazonS3Client,
                                                        String bucketName,
                                                        String backupName) {
        Map<String, Long> snapshotFiles = new HashMap<> ( );
        try {
            final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName).withPrefix(backupName);
            ListObjectsV2Result result;
            do {
                result = amazonS3Client.listObjectsV2(req);
                for (S3ObjectSummary objectSummary :
                        result.getObjectSummaries()) {
                    snapshotFiles.put ( objectSummary.getKey ( ), objectSummary.getSize());
                }
                req.setContinuationToken(result.getNextContinuationToken());
            } while(result.isTruncated());
        } catch (Exception e) {
            LOGGER.error("Unable to list snapshot files: " + e);
        }
        return snapshotFiles;
    }
}
