package org.apache.mesos.offer;

import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Offer.Operation;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.protobuf.OperationBuilder;

import java.util.List;

/**
 * Create OfferRecommendation.
 * This Recommendation encapsulates a Mesos CREATE Operation
 */
public class CreateOfferRecommendation implements OfferRecommendation {
  private OperationBuilder builder;
  private Offer offer;
  private List<Resource> volumes;
  private VolumeRequirement volumeRequirement;

  public CreateOfferRecommendation(
          Offer offer,
          List<Resource> volumes,
          VolumeRequirement volumeRequirement) {
    builder = new OperationBuilder();
    builder.setType(Operation.Type.CREATE);
    this.offer = offer;
    this.volumes = volumes;
    this.volumeRequirement = volumeRequirement;
  }

  public Operation getOperation() {
    volumes = ResourceUtils.updateVolumeDiskInfos(volumeRequirement, volumes, offer);
    builder.setCreate(volumes);
    return builder.build();
  }

  public Offer getOffer() {
    return offer;
  }
}
