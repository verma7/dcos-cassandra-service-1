package org.apache.mesos.offer;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Offer.Operation;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.protobuf.OperationBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Launch OfferRecommendation.
 * This Recommendation encapsulates a Mesos LAUNCH Operation
 */
public class LaunchOfferRecommendation implements OfferRecommendation {
  private OperationBuilder builder;
  private Offer offer;
  private Collection<TaskInfo> taskInfos;
  private VolumeRequirement volumeRequirement;

  public LaunchOfferRecommendation(
          Offer offer,
          Collection<TaskInfo> taskInfos,
          VolumeRequirement volumeRequirement) {
    builder = new OperationBuilder();
    builder.setType(Operation.Type.LAUNCH);
    this.offer = offer;
    this.taskInfos = taskInfos;
    this.volumeRequirement = volumeRequirement;
  }

  public Operation getOperation() {
    Collection<TaskInfo> inTaskInfos = taskInfos;
    Collection<TaskInfo> outTaskInfos = new ArrayList<TaskInfo>();

    SlaveID agentId = offer.getSlaveId();
    for (TaskInfo taskInfo : inTaskInfos) {
      TaskInfo.Builder builder = TaskInfo.newBuilder(taskInfo);
      builder.setSlaveId(agentId);
      if (volumeRequirement != null) {
        final List<Protos.Resource> resources =
                ResourceUtils.updateVolumeDiskInfos(volumeRequirement, builder.getResourcesList(), offer);
        builder.clearResources();
        builder.addAllResources(resources);
      }
      outTaskInfos.add(builder.build());
    }

    builder.setLaunch(outTaskInfos);
    return builder.build();
  }

  public Offer getOffer() {
    return offer;
  }
}
