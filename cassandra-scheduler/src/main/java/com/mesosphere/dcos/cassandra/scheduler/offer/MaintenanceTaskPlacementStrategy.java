package com.mesosphere.dcos.cassandra.scheduler.offer;

import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.PlacementStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This is the placement strategy for Cassandra maintenance task, which we want to run
 * on the same Cassandra node that needs to be maintained. Examples of maintenance tasks
 * are repair, backup and restore, etc.
 */
public class MaintenanceTaskPlacementStrategy implements PlacementStrategy {

    private CassandraTasks cassandraTasks;

    public MaintenanceTaskPlacementStrategy(CassandraTasks cassandraTasks) {
        this.cassandraTasks = cassandraTasks;
    }

    @Override
    public List<Protos.SlaveID> getAgentsToAvoid(Protos.TaskInfo taskInfo) {
        final String thisAgentID = taskInfo.getSlaveId().getValue();

        return cassandraTasks.getDaemons().values().stream()
                .filter(task -> !task.getSlaveId().equals(thisAgentID))
                .map(task -> task.toProto().getSlaveId())
                .collect(Collectors.toList());
    }

    @Override
    public List<Protos.SlaveID> getAgentsToColocate(Protos.TaskInfo taskInfo) {
        return Collections.singletonList(taskInfo.getSlaveId());
    }
}
