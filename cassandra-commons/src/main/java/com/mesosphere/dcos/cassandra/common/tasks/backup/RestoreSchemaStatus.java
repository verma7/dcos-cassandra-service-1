package com.mesosphere.dcos.cassandra.common.tasks.backup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskStatus;
import org.apache.mesos.Protos;

import java.util.Optional;

/**
 * RestoreSchemaStatus extends CassandraTaskStatus to implement the status
 * Object for RestoreSchemaTask.
 */
public class RestoreSchemaStatus  extends CassandraTaskStatus {
    /**
     * Creates a RestoreSchemaStatus.
     * @param state      The state of the task
     * @param id         The id of the task associated with the status.
     * @param slaveId    The id of the slave on which the task associated
     *                   with the status was launched.
     * @param executorId The id of the executor for the task associated with
     *                   the status.
     * @param message    An optional message sent from the executor.
     * @return A RestoreSchemaStatus constructed from the parameters.
     */
    @JsonCreator
    public static RestoreSchemaStatus create(
            @JsonProperty("state") Protos.TaskState state,
            @JsonProperty("id") String id,
            @JsonProperty("slave_id") String slaveId,
            @JsonProperty("executor_id") String executorId,
            @JsonProperty("message") Optional<String> message) {
        return new RestoreSchemaStatus(state, id, slaveId, executorId,
                message);
    }

    /**
     * Constructs a RestoreSchemaStatus
     * @param state      The state of the task
     * @param id         The id of the task associated with the status.
     * @param slaveId    The id of the slave on which the task associated
     *                   with the status was launched.
     * @param executorId The id of the executor for the task associated with
     *                   the status.
     * @param message    An optional message sent from the executor.
     */
    protected RestoreSchemaStatus(Protos.TaskState state,
                                    String id,
                                    String slaveId,
                                    String executorId,
                                    Optional<String> message) {
        super(CassandraTask.TYPE.SCHEMA_RESTORE,
                state,
                id,
                slaveId,
                executorId,
                message);
    }

    @Override
    public RestoreSchemaStatus update(Protos.TaskState state) {
        if (isFinished()) {
            return this;
        } else {
            return create(state, id, slaveId, executorId, message);
        }
    }

    @Override
    protected CassandraProtos.CassandraTaskStatusData getData() {
        return CassandraProtos.CassandraTaskStatusData.newBuilder()
                .setType(
                        CassandraProtos.CassandraTaskData.TYPE.SCHEMA_RESTORE)
                .build();
    }
}
