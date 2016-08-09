package org.apache.mesos.offer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Resource.DiskInfo;
import org.apache.mesos.Protos.Value.Range;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * ResourceUtils.
 */
public class ResourceUtils {
  private static final Log LOGGER = LogFactory.getLog(ResourceUtils.class);

  private static boolean filterDisk(final Resource resource,
                                    final VolumeRequirement.VolumeType type) {
    if (isDisk(resource)) {
      final DiskInfo.Source.Type source = getDiskSourceType(type);
      if (source == null && (!resource.hasDisk() || !resource.getDisk().hasSource())) {
        LOGGER.info(
            String.format("Selected disk: " +
              "type = %s" +
              "resource = %s",
              type,
              resource));
        return true;
      } else if (resource.hasDisk() &&
          resource.getDisk().hasSource() &&
          source == resource.getDisk().getSource().getType()) {
        LOGGER.info(
            String.format("Selected disk: " +
              "type = %s" +
              "resource = %s",
              type,
              resource));
        return true;
      } else {
        LOGGER.info(
            String.format("Filtered disk: " +
              "type = %s" +
              "resource = %s",
              type,
              resource));
        return false;
      }
    } else {
      return false;
    }
  }

  public static final String DISK = "disk";
  public static final String CPUS = "cpus";
  public static final String MEM = "mem";

  public static boolean isDisk(final Resource resource) {
    return resource != null && DISK.equalsIgnoreCase(resource.getName());
  }

  public static boolean isMem(final Resource resource) {
    return resource != null && MEM.equalsIgnoreCase(resource.getName());
  }

  public static boolean isCpus(final Resource resource) {
    return resource != null && CPUS.equalsIgnoreCase(resource.getName());
  }


  public static double getUnreservedCpu(List<Resource> resources) {
    return getUnreservedScalarResource(CPUS, resources);
  }

  public static double getReservedCpu(
    List<Resource> resources,
    String role,
    String principal) {
    return getReservedScalarResource(CPUS, resources, role, principal);
  }

  public static double getUnreservedMem(List<Resource> resources) {
    return getUnreservedScalarResource(MEM, resources);
  }

  public static double getReservedMem(
    List<Resource> resources,
    String role,
    String principal) {
    return getReservedScalarResource(MEM, resources, role, principal);
  }

  public static double getUnreservedDisk(List<Resource> resources) {
    return getUnreservedScalarResource(DISK, resources);
  }

  public static DiskInfo.Source.Type getDiskSourceType(
          VolumeRequirement.VolumeType type) {
    switch (type) {
      case PATH:
        return DiskInfo.Source.Type.PATH;
      case MOUNT:
        return DiskInfo.Source.Type.MOUNT;
      default:
        return null;
    }
  }

  public static List<Resource> filterDiskByVolumeType(
          final List<Resource> resources,
          final VolumeRequirement.VolumeType type) {

    return resources.stream()
            .filter(resource -> filterDisk(resource, type))
            .collect(Collectors.toList());
  }

  public static double getTotalReservedDisk(
    List<Resource> resources,
    String role,
    String principal) {
    return getAvailableReservedDisk(resources, role, principal) +
           getVolumeReservedDisk(resources, role, principal);
  }

  public static double getAvailableReservedDisk(
    List<Resource> resources,
    String role,
    String principal) {

    double reserved = 0.0;

    for (Resource resource : getReservedResources(resources, role, principal)) {
      if (resource.getName().equals(DISK) && !resource.hasDisk()) {
        reserved += resource.getScalar().getValue();
      }
    }

    return reserved;
  }

  public static double getVolumeReservedDisk(
    List<Resource> resources,
    String role,
    String principal) {

    double reserved = 0.0;

    for (Resource resource : getReservedResources(resources, role, principal)) {
      if (resource.getName().equals(DISK) && resource.hasDisk()) {
        reserved += resource.getScalar().getValue();
      }
    }

    return reserved;
  }

  public static List<Range> getUnreservedPorts(List<Resource> resources) {
    return getUnreservedRangeResource("ports", resources);
  }

  public static List<Range> getReservedPorts(
    List<Resource> resources,
    String role,
    String principal) {
    return getReservedRangeResource("ports", resources, role, principal);
  }

  public static String getVolumeContainerPath(List<Resource> resources) {
    for (Resource resource : resources) {
      if (resource.hasDisk()) {
        DiskInfo diskInfo = resource.getDisk();
        if (diskInfo.hasVolume()) {
          return diskInfo.getVolume().getContainerPath();
        }
      }
    }

    return null;
  }

  public static List<Resource> filterUnreservedDiskResources(List<Resource> resources) {
    List<Resource> diskResources = new ArrayList<>();
    for (Resource resource : resources) {
      if ((DISK.equals(resource.getName()) || resource.hasDisk()) && resource.getRole().equals("*")) {
        diskResources.add(resource);
      }
    }
    return diskResources;
  }

  public static List<Resource> filterReservedDiskResources(
          final List<Resource> resources,
          final String role,
          final String principal) {
    List<Resource> diskResources = new ArrayList<>();
    final List<Resource> reservedResources = getReservedResources(resources, role, principal);
    for (Resource resource : reservedResources) {
      if (DISK.equals(resource.getName())) {
        diskResources.add(resource);
      }
    }
    return diskResources;
  }

  public static List<Resource> updateVolumeDiskInfos(
          VolumeRequirement volumeRequirement,
          List<Resource> taskResources,
          Protos.Offer offer) {
    if (volumeRequirement == null) {
      return taskResources;
    }

    final VolumeRequirement.VolumeType volumeType = volumeRequirement.getVolumeType();

    if (!VolumeRequirement.VolumeType.MOUNT.equals(volumeType)) {
      LOGGER.info("Volume requirement is not MOUNT: No update required");
      return taskResources;
    }

    final Optional<Resource> resourceOption = ResourceUtils
      .filterDiskByVolumeType(
          offer.getResourcesList(),
          volumeType).stream().findFirst();

    if (!resourceOption.isPresent()) {
      LOGGER.info("No mount disk found returning unmodified resources");
      return taskResources;
    }

    final Resource resourceFromOffer = resourceOption.get();
    final Resource.DiskInfo diskInfoFromOffer = resourceFromOffer.getDisk();
    final Resource.DiskInfo.Source sourceFromOffer = diskInfoFromOffer.getSource();

    return taskResources.stream().map(resource -> {
      if (!isDisk(resource)) {
        return resource;
      } else {
        LOGGER.info(
          String.format("Found MOUNT disk: updating task " +
            "resource: resource = %s",
            resource));

        final Resource.DiskInfo taskDiskInfo = resource.getDisk();
        final Protos.Value.Scalar scalarFromOffer = resourceFromOffer.getScalar();

        final Resource.Builder diskWithSourceBuilder = Resource.newBuilder(resource)
                .setScalar(scalarFromOffer);

        final Resource.DiskInfo diskInfoWithSource =
                Resource.DiskInfo.newBuilder(taskDiskInfo).setSource(sourceFromOffer).build();
        diskWithSourceBuilder.setDisk(diskInfoWithSource);
        Resource mutated = diskWithSourceBuilder.build();

        LOGGER.info(
          String.format("Updated resource: previous = %s " +
            "updated = %s",
            resource,
            mutated));

        return mutated;
      }
    }).collect(Collectors.toList());
  }

  private static double getUnreservedScalarResource(String resourceName, List<Resource> resources) {
    double unreserved = 0.0;

    for (Resource resource : resources) {
      if (resource.getName().equals(resourceName) &&
        resource.getRole().equals("*")) {
        unreserved += resource.getScalar().getValue();
      }
    }

    return unreserved;
  }

  private static double getReservedScalarResource(
    String resourceName,
    List<Resource> resources,
    String role,
    String principal) {

    double reserved = 0.0;

    for (Resource resource : getReservedResources(resources, role, principal)) {
      if (resource.getName().equals(resourceName)) {
        reserved += resource.getScalar().getValue();
      }
    }

    return reserved;
  }

  private static List<Range> getUnreservedRangeResource(String resourceName,
                                                        List<Resource> resources) {
    List<Range> ranges = new ArrayList<Range>();

    for (Resource resource : resources) {
      if (resource.getName().equals(resourceName) &&
        resource.getRole().equals("*")) {
        ranges.addAll(resource.getRanges().getRangeList());
      }
    }

    return ranges;
  }

  private static List<Range> getReservedRangeResource(
    String resourceName,
    List<Resource> resources,
    String role,
    String principal) {

    List<Range> ranges = new ArrayList<Range>();

    for (Resource resource : getReservedResources(resources, role, principal)) {
      if (resource.getName().equals(resourceName)) {
        ranges.addAll(resource.getRanges().getRangeList());
      }
    }

    return ranges;
  }

  private static List<Resource> getReservedResources(
      List<Resource> resources,
      String role,
      String principal) {

    List<Resource> outResources = new ArrayList<Resource>();

    for (Resource resource : resources) {
      if (resource.getRole().equals(role) &&
          resource.getReservation().getPrincipal().equals(principal)) {
        outResources.add(resource);
      }
    }

    return outResources;
  }
}
