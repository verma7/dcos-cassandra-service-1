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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mesosphere.dcos.cassandra.common.serialization.SerializationException;
import com.mesosphere.dcos.cassandra.common.serialization.Serializer;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskContext;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * BackupContext implements ClusterTaskContext to provide a context for
 * cluster wide backup operations.
 */
public class BackupContext implements ClusterTaskContext {

    /**
     * Serializer serializes and deserializes a BackupContext to and from a
     * JSON Object.
     */
    public static final Serializer<BackupContext> JSON_SERIALIZER =
            new Serializer<BackupContext>() {
                @Override
                public byte[] serialize(BackupContext value)
                        throws SerializationException {
                    try {
                        return JsonUtils.MAPPER.writeValueAsBytes(value);
                    } catch (JsonProcessingException ex) {
                        throw new SerializationException(
                                "Error writing BackupContext to JSON",
                                ex);
                    }
                }

                @Override
                public BackupContext deserialize(byte[] bytes)
                        throws SerializationException {
                    try {
                        return JsonUtils.MAPPER.readValue(bytes,
                                BackupContext.class);
                    } catch (IOException ex) {
                        throw new SerializationException("Error reading " +
                                "BackupContext form JSON", ex);
                    }
                }
            };

    private String nodeId;
    private String name;
    private String externalLocation;
    private String localLocation;
    private String s3AccessKey;
    private String s3SecretKey;
    private List<String> keySpaces;

    /**
     * Gets the name of the backup.
     *
     * @return The name of the backup.
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the external location of the backup.
     *
     * @return The location where the backup files are stored.
     */
    public String getExternalLocation() {
        return externalLocation;
    }

    /**
     * Gets the local location of the backup.
     *
     * @return The local location of the keyspace files that will be backed up.
     */
    public String getLocalLocation() {
        return localLocation;
    }

    /**
     * Gets the access key.
     *
     * @return The S3 access key for the bucket where the keyspace files will
     * be stored.
     */
    public String getS3AccessKey() {
        return s3AccessKey;
    }

    /**
     * Gets the secret key.
     *
     * @return The S3 secret key for the bucket where the keyspace files will
     * be stored.
     */
    public String getS3SecretKey() {
        return s3SecretKey;
    }

    /**
     * Gets the list of keyspaces to be backed up if null then
     * all keyspaces will be backed up
     *
     * @return keyspaces to be backed up
     */
    public List<String> getKeySpaces() { return keySpaces; }

    /**
     * Sets the backup name.
     *
     * @param name The name of the backup.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Sets the external location.
     *
     * @param externalLocation The location where the backup files will be
     *                         stored.
     */
    public void setExternalLocation(String externalLocation) {
        this.externalLocation = externalLocation;
    }

    /**
     * Sets the local location.
     *
     * @param localLocation The location where the keyspace files will be
     *                      read from.
     */
    public void setLocalLocation(String localLocation) {
        this.localLocation = localLocation;
    }

    /**
     * Sets the S3 access key.
     *
     * @param s3AccessKey The access key for the bucket where the backup
     *                    files will be stored.
     */
    public void setS3AccessKey(String s3AccessKey) {
        this.s3AccessKey = s3AccessKey;
    }

    /**
     * Sets the S3 secret key.
     *
     * @param s3SecretKey The secret key for the bucket where teh backup
     *                    files will be stored.
     */
    public void setS3SecretKey(String s3SecretKey) {
        this.s3SecretKey = s3SecretKey;
    }

    /**
     * sets the keyspaces to be backed up
     *
     * @param keySpaces the keyspaces to be backed
     */
    public void setKeySpaces(List<String> keySpaces) { this.keySpaces = keySpaces; }

    /**
     * Gets the id of the node for the backup.
     *
     * @return The id of the node for the backup.
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * Sets the id of the node for the backup.
     *
     * @param nodeId The id of the node for the backup.
     */
    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BackupContext)) return false;
        BackupContext that = (BackupContext) o;
        return Objects.equals(getNodeId(), that.getNodeId()) &&
                Objects.equals(getName(), that.getName()) &&
                Objects.equals(getExternalLocation(),
                        that.getExternalLocation()) &&
                Objects.equals(getLocalLocation(),
                        that.getLocalLocation()) &&
                Objects.equals(getS3AccessKey(), that.getS3AccessKey()) &&
                Objects.equals(getS3SecretKey(), that.getS3SecretKey()) &&
                Objects.equals(getKeySpaces(), that.getKeySpaces());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNodeId(), getName(), getExternalLocation(),
                getLocalLocation(), getS3AccessKey(), getS3SecretKey(), getKeySpaces());
    }
}
