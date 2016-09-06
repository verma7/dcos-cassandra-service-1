package com.mesosphere.dcos.cassandra.scheduler.offer;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.Protos;
import org.apache.mesos.protobuf.TaskInfoBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MaintenanceTaskPlacementStrategyTest {

    @Mock
    private CassandraTasks cassandraTasks;

    private Protos.TaskInfo thisTaskInfo = new TaskInfoBuilder("taskid", "repair", "node_i_need").build();
    private MaintenanceTaskPlacementStrategy placementStrategy;

    @Before
    public void setup() throws InvalidProtocolBufferException {
        MockitoAnnotations.initMocks(this);

        Map<String, CassandraDaemonTask> daemonTaskMap = ImmutableMap.<String, CassandraDaemonTask>builder()
                .put("node_i_want_to_avoid", mockCassandraDaemonTask("node_i_want_to_avoid"))
                .put("node_i_need", mockCassandraDaemonTask("node_i_need"))
                .build();

        when(cassandraTasks.getDaemons()).thenReturn(daemonTaskMap);
        placementStrategy = new MaintenanceTaskPlacementStrategy(cassandraTasks);
    }

    @Test
    public void getAgentsToAvoid_shouldReturnExpectedAgents() {
        List<Protos.SlaveID> slaveIDList = placementStrategy.getAgentsToAvoid(thisTaskInfo);
        assertTrue(slaveIDList.size() == 1);
        assertTrue(slaveIDList.get(0).getValue().equals("node_i_want_to_avoid"));
    }

    @Test
    public void getAgentsToColocate_shouldReturnNull() {
        List<Protos.SlaveID> slaveIDList = placementStrategy.getAgentsToColocate(thisTaskInfo);
        assertTrue(slaveIDList.size() == 1);
        assertTrue(slaveIDList.get(0).getValue().equals("node_i_need"));
    }

    private CassandraDaemonTask mockCassandraDaemonTask(String agentID) {
        CassandraDaemonTask cassandraDaemonTask = mock(CassandraDaemonTask.class);
        when(cassandraDaemonTask.getSlaveId()).thenReturn(agentID);

        Protos.TaskInfo taskInfo = new TaskInfoBuilder("taskid", "cassandra_daemon", agentID).build();
        when(cassandraDaemonTask.toProto()).thenReturn(taskInfo);
        return cassandraDaemonTask;
    }
}
