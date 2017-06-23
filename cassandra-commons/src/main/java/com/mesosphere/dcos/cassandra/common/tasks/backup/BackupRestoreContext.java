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
package com.mesosphere.dcos.cassandra.common.tasks.backup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskContext;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.util.List;
import java.util.Objects;

/**
 * BackupRestoreContext implements ClusterTaskContext to provide a context for
 * cluster wide restore operations.
 */
public class BackupRestoreContext implements ClusterTaskContext {

    @JsonCreator
    public static final BackupRestoreContext create(
        @JsonProperty("node_id")
        final String nodeId,
        @JsonProperty("name")
        final String name,
        @JsonProperty("external_location")
        final String externalLocation,
        @JsonProperty("local_location")
        final String localLocation,
        @JsonProperty("account_id")
        final String accountId,
        @JsonProperty("secret_key")
        final String secretKey,
        @JsonProperty("uses_emc")
        final boolean usesEmc,
        @JsonProperty("restore_type")
        final String restoreType,
        @JsonProperty("key_spaces")
        final List<String> keyspaces,
        @JsonProperty("min_free_space_percent")
        final float minFreeSpacePercent,
        @JsonProperty("persistent_volume_id")
        final String persistentVolumeId,
        @JsonProperty("public_key_path")
        final String publicKeyPath,
        @JsonProperty("private_key_path")
        final String privateKeyPath) {

        return new BackupRestoreContext(
            nodeId,
            name,
            externalLocation,
            localLocation,
            accountId,
            secretKey,
            usesEmc,
            restoreType,
            keyspaces,
            minFreeSpacePercent,
            persistentVolumeId,
            publicKeyPath,
            privateKeyPath);
    }

    @JsonProperty("node_id")
    private final String nodeId;

    @JsonProperty("name")
    private final String name;

    @JsonProperty("external_location")
    private final String externalLocation;

    @JsonProperty("local_location")
    private final String localLocation;

    @JsonProperty("account_id")
    private final String accountId;  // s3AccessKey or AccountName (prinicipal for service)

    @JsonProperty("secret_key")
    private final String secretKey;

    @JsonProperty("uses_emc")
    private final boolean usesEmc;

    @JsonProperty("restore_type")
    private final String restoreType;

    @JsonProperty("key_spaces")
    private final List<String> keySpaces;

    @JsonProperty("min_free_space_percent")
    private final float minFreeSpacePercent;

    @JsonProperty("persistent_volume_id")
    private final String persistentVolumeId;

    @JsonProperty("public_key_path")
    private final String publicKeyPath;

    @JsonProperty("private_key_path")
    private final String privateKeyPath;

    public BackupRestoreContext(final String nodeId,
                                final String name,
                                final String externalLocation,
                                final String localLocation,
                                final String accountId,
                                final String secretKey,
                                final boolean usesEmc,
                                final String restoreType,
                                final List<String> keySpaces,
                                final float minFreeSpacePercent,
                                final String persistentVolumeId,
                                final String publicKeyPath,
                                final String privateKeyPath) {
        this.nodeId = nodeId;
        this.externalLocation = externalLocation;
        this.name = name;
        this.localLocation = localLocation;
        this.accountId = accountId;
        this.secretKey = secretKey;
        this.usesEmc = usesEmc;
        this.restoreType = restoreType;
        this.keySpaces = keySpaces;
        this.minFreeSpacePercent = minFreeSpacePercent;
        this.persistentVolumeId = persistentVolumeId;
        this.publicKeyPath = publicKeyPath;
        this.privateKeyPath = privateKeyPath;
    }

    /**
     * Gets the name of the backup.
     *
     * @return The name of the backup.
     */
    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("uses_emc")
    public boolean getUsesEmc() {
        return usesEmc;
    }

    public boolean usesEmc() {
        return getUsesEmc();
    }

    /**
     * Gets the external location of the backup.
     *
     * @return The location where the backup files are stored.
     */
    @JsonProperty("external_location")
    public String getExternalLocation() {
        return externalLocation;
    }

    /**
     * Gets the local location of the backup.
     *
     * @return The local location where the backup files will be downloaded to.
     */
    @JsonProperty("local_location")
    public String getLocalLocation() {
        return localLocation;
    }

    /**
     * Gets the access key.
     *
     * @return The S3 access key for the bucket or Azure account where the keyspace files are
     * be stored.
     */
    @JsonProperty("account_id")
    public String getAccountId() {
        return accountId;
    }

    /**
     * Gets the secret key.
     *
     * @return The S3 secret key for the bucket or azure key where the keyspace files are
     * be stored.
     */
    @JsonProperty("secret_key")
    public String getSecretKey() {
        return secretKey;
    }

    /**
     * Gets the id of the node for the backup.
     *
     * @return The id of the node for the backup.
     */
    @JsonProperty("node_id")
    public String getNodeId() {
        return nodeId;
    }

    /**
     * Gets the cluster setup for restoring
     * either existing or completely new one.
     * @return
     */
    @JsonProperty("restore_type")
    public String getRestoreType() { return restoreType; }

    @JsonProperty("key_spaces")
    public List<String> getKeySpaces() {
        return keySpaces;
    }

    /**
     * Gets the free space requirement of the node for the backup.
     *
     * @return The free space requirement of the node for the backup in percentage.
     */
    @JsonProperty("min_free_space_percent")
    public float getMinFreeSpacePercent() {
        return minFreeSpacePercent;
    }

    /**
     * Gets the persistent volume id of the node for the backup.
     *
     * @return The persistent volume id of the node for the backup in percentage.
     */
    @JsonProperty("persistent_volume_id")
    public String getPersistentVolumeId() {
        return persistentVolumeId;
    }

    @JsonProperty("public_key_path")
    public String getPublicKeyPath() {return publicKeyPath; }

    @JsonProperty("private_key_path")
    public String getPrivateKeyPath() { return privateKeyPath; }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BackupRestoreContext)) return false;
        BackupRestoreContext that = (BackupRestoreContext) o;
        return Objects.equals(getNodeId(), that.getNodeId()) &&
                Objects.equals(getName(), that.getName()) &&
                Objects.equals(getExternalLocation(),
                        that.getExternalLocation()) &&
                Objects.equals(getLocalLocation(),
                        that.getLocalLocation()) &&
                Objects.equals(getAccountId(), that.getAccountId()) &&
                Objects.equals(getSecretKey(), that.getSecretKey()) &&
                Objects.equals(getRestoreType(), that.getRestoreType()) &&
                Objects.equals(getKeySpaces(), that.getKeySpaces()) &&
                Objects.equals(getMinFreeSpacePercent(), that.getMinFreeSpacePercent()) &&
                Objects.equals(getPersistentVolumeId(), that.getPersistentVolumeId()) &&
                Objects.equals(getPublicKeyPath(), that.getPublicKeyPath()) &&
                Objects.equals(getPrivateKeyPath(), that.getPrivateKeyPath());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNodeId(), getName(), getExternalLocation(),
                getLocalLocation(), getAccountId(), getSecretKey(), getRestoreType(),
                getKeySpaces(), getMinFreeSpacePercent(), getPersistentVolumeId(),
                getPublicKeyPath(), getPrivateKeyPath());
    }

    @JsonIgnore
    public BackupRestoreContext forNode(final String nodeId){
        return create(
            nodeId,
            name,
            externalLocation,
            localLocation,
            accountId,
            secretKey,
            usesEmc,
            restoreType,
            keySpaces,
            minFreeSpacePercent,
            persistentVolumeId,
            publicKeyPath,
            privateKeyPath);
    }

    @JsonIgnore
    public BackupRestoreContext withLocalLocation(final String localLocation){
        return create(
            nodeId,
            name,
            externalLocation,
            localLocation,
            accountId,
            secretKey,
            usesEmc,
            restoreType,
            keySpaces,
            minFreeSpacePercent,
            persistentVolumeId,
            publicKeyPath,
            privateKeyPath);
    }
}
