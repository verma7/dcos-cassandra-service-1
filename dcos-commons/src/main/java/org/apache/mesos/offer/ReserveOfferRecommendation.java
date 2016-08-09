package org.apache.mesos.offer;

import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Offer.Operation;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.protobuf.OperationBuilder;
import org.apache.mesos.protobuf.ResourceBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * ReserveOfferRecommendation.
 * This Recommendation encapsulates a Mesos RESERVE Operation
 */
public class ReserveOfferRecommendation implements OfferRecommendation {
    private OperationBuilder builder;
    private Offer offer;
    private String role;
    private String principal;
    private double cpu;
    private double mem;
    private double disk;
    private List<Range> ports;
    private Resource.DiskInfo diskInfo;

    private double computeDisk(double disk, Offer offer, Resource.DiskInfo
            diskInfo) {
        final Optional<Resource> mountDisk = ResourceUtils.
                filterDiskByVolumeType(offer.getResourcesList(),
                        VolumeRequirement.VolumeType.MOUNT).stream()
                .findFirst();
        if (diskInfo != null &&
                diskInfo.hasSource()
                && diskInfo.getSource() != null
                && diskInfo.getSource().hasType()
                && diskInfo.getSource().getType() ==
                Resource.DiskInfo.Source.Type.MOUNT &&
                mountDisk.isPresent()) {
            // Use entire disk
            return mountDisk.get().getScalar().getValue();
        } else {
            return disk;
        }
    }

    public ReserveOfferRecommendation(
            Offer offer,
            String role,
            String principal,
            double cpu,
            double mem,
            double disk,
            List<Range> ports,
            Resource.DiskInfo diskInfo) {
        builder = new OperationBuilder();
        builder.setType(Operation.Type.RESERVE);

        this.offer = offer;
        this.role = role;
        this.principal = principal;
        this.cpu = cpu;
        this.mem = mem;
        this.disk = disk;
        this.ports = ports;
        this.diskInfo = diskInfo;
        this.disk = computeDisk(disk, offer, diskInfo);

    }


    public Operation getOperation() {
        builder.setReserve(getReservedResources());
        return builder.build();
    }

    public Offer getOffer() {
        return offer;
    }

    private List<Resource> getReservedResources() {
        List<Resource> resources = new ArrayList<Resource>();

        if (cpu > 0) {
            resources.add(getReservedCpuResource());
        }

        if (mem > 0) {
            resources.add(getReservedMemResource());
        }

        if (disk > 0) {
            resources.add(getReservedDiskResource());
        }

        if (ports.size() > 0) {
            resources.add(getReservedPortsResource());
        }

        return resources;
    }

    private Resource getReservedCpuResource() {
      return ResourceBuilder.reservedCpus(cpu, role, principal);
    }

    private Resource getReservedMemResource() {
      return ResourceBuilder.reservedMem(mem, role, principal);
    }

    private Resource getReservedDiskResource() {
      // If disk type is ROOT, then we are only going to reserve the configured diskMb.
      // If disk type is MOUNT, then we are required by Mesos to consume the entire MOUNT disk.
      return ResourceBuilder.reservedDisk(disk, role, principal, diskInfo);
    }

    private Resource getReservedPortsResource() {
      return ResourceBuilder.reservedPorts(ports, role, principal);
    }
}
