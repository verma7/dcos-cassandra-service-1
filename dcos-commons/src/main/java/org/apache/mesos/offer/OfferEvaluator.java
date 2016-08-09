package org.apache.mesos.offer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Resource.DiskInfo;
import org.apache.mesos.Protos.Resource.DiskInfo.Persistence;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.util.Algorithms;

import java.util.*;

/**
 * The OfferEvaluator processes Offers and produces OfferRecommendations.
 * The determination of what OfferRecommendations, if any should be made are made
 * in reference to the OfferRequirement with which it was constructed.  In the
 * case where an OfferRequirement has not been provided no OfferRecommendations
 * are ever returned.
 */
public class OfferEvaluator {
  private final Log log = LogFactory.getLog(OfferEvaluator.class);

  private OfferRequirement requirement;

  public OfferEvaluator() {
    this.requirement = null;
  }

  public OfferEvaluator(OfferRequirement requirement) {
    this.requirement = requirement;
  }

  public void setOfferRequirement(OfferRequirement requirement) {
    this.requirement = requirement;
  }

  public List<OfferRecommendation> evaluate(List<Offer> offers) {
    if (requirement == null) {
      log.warn("No requirement to meet.");
      return Collections.emptyList();
    }

    List<Offer> acceptableOffers = getAcceptablePlacementConstraintsOffers(offers);

    if (acceptableOffers.size() > 0) {
      acceptableOffers = getSufficientResourceOffers(acceptableOffers);
    } else {
      log.warn("No acceptable offers due to placement constraints.");
      log.warn("Needed to avoid: " + requirement.getAvoidAgents());
      log.warn("Needed to colocate: " + requirement.getColocateAgents());
      return Collections.emptyList();
    }

    if (acceptableOffers.size() <= 0) {
      log.warn("No acceptable offers due to insufficient resources.");
      log.warn("Needed resources: " + requirement.getTaskInfos());
      return Collections.emptyList();
    }

    log.info(String.format("Found '%s' acceptable offers.", acceptableOffers.size()));
    Offer offerToAccept = acceptableOffers.get(0);
    return getRecommendations(offerToAccept);
  }

  private List<OfferRecommendation> getRecommendations(Offer offer) {
    List<OfferRecommendation> recommendations = new ArrayList<OfferRecommendation>();

    OfferRecommendation recommendation = getReserveOfferRecommendation(offer);
    if (recommendation != null) {
      recommendations.add(recommendation);
    }

    final VolumeRequirement volumeRequirement = requirement.getVolumeRequirement();
    final VolumeRequirement.VolumeMode volumeMode = volumeRequirement.getVolumeMode();

    if (volumeMode == VolumeRequirement.VolumeMode.CREATE) {
      final List<Resource> volumes = requirement.getVolumes();
      recommendations.add(new CreateOfferRecommendation(offer, volumes, volumeRequirement));
    }

    recommendations.add(new LaunchOfferRecommendation(offer, requirement.getTaskInfos(), volumeRequirement));

    return recommendations;
  }

  private OfferRecommendation getReserveOfferRecommendation(Offer offer) {
    double neededCpu = getNeededReservedCpu(offer);
    double neededMem = getNeededReservedMem(offer);
    double neededDisk = getNeededReservedDisk(offer);
    List<Range> neededPorts = getNeededReservedPorts(offer);
    final VolumeRequirement.VolumeType volumeType = requirement.getVolumeRequirement().getVolumeType();

    final Optional<DiskInfo> info =
            ResourceUtils.filterDiskByVolumeType(
                    offer.getResourcesList(),
                    volumeType).stream()
                    .findFirst()
                    .flatMap(resource -> {
                      return resource.hasDisk() ? Optional.of(
                              resource.getDisk()) :
                              Optional.empty();
                    });

    log.info("Filtered disk for type: " + volumeType + " is: " + info);

    if (neededCpu > 0 || neededMem > 0 || neededDisk > 0 || neededPorts.size() > 0) {
      return new ReserveOfferRecommendation(
        offer,
        requirement.getRole(),
        requirement.getPrincipal(),
        neededCpu,
        neededMem,
        neededDisk,
        neededPorts,
        info.isPresent() ? info.get() : null);
    } else {
      return null;
    }
  }

  private List<Offer> getSufficientResourceOffers(List<Offer> offers) {
    List<Offer> acceptableOffers = new ArrayList<Offer>();

    for (Offer offer : offers) {
      boolean enoughCpu = enoughCpu(offer);
      boolean enoughMem = enoughMem(offer);
      boolean enoughDisk = enoughDisk(offer);
      boolean enoughPorts = enoughPorts(offer);
      boolean hasExpectedVolumes = hasExpectedVolumes(offer);

      log.info("EnoughCPU: " + enoughCpu + " EnoughMem: " + enoughMem + " EnoughDisk: " + enoughDisk
              + " EnoughPorts: " + enoughPorts + " HasExpectedVolumes: " + hasExpectedVolumes);

      if (enoughCpu && enoughMem && enoughDisk && enoughPorts && hasExpectedVolumes) {
        acceptableOffers.add(offer);
      }
    }

    if (acceptableOffers.size() > 0) {
      for (Offer offer : acceptableOffers) {
        log.info("Found Offer meeting Resource constraints: " + offer);
      }
    } else {
      log.warn("No Offers found meeting Resource constraints.");
    }

    return acceptableOffers;
  }

  private boolean hasExpectedVolumes(Offer offer) {
    boolean hasExpectedVolumes = false;
    final VolumeRequirement volumeRequirement = requirement.getVolumeRequirement();
    final VolumeRequirement.VolumeMode volumeMode = volumeRequirement.getVolumeMode();
    final VolumeRequirement.VolumeType volumeType = volumeRequirement.getVolumeType();

    if (VolumeRequirement.VolumeMode.NONE == volumeMode) {
      hasExpectedVolumes = true;
    } else if (VolumeRequirement.VolumeMode.CREATE == volumeMode) {
      if (volumeType == VolumeRequirement.VolumeType.MOUNT ||
          volumeType == VolumeRequirement.VolumeType.ROOT) {
        hasExpectedVolumes = true;
      } else {
        log.error("VolumeType " + volumeType + " is not supported for volumeMode " + volumeMode);
      }
    } else if (VolumeRequirement.VolumeMode.EXISTING == volumeMode) {
      hasExpectedVolumes = hasVolumeIds(
          ResourceUtils.filterDiskByVolumeType(
            offer.getResourcesList(), volumeType));
    }

    log.info(String.format("VolumeMode is %s and VolumeType is %s hasExpectedVolumes is %b",
            volumeMode.name(),
            volumeType.name(),
            hasExpectedVolumes));

    return hasExpectedVolumes;
  }

  private boolean hasVolumeIds(List<Resource> resourcesFromOffer) {
    List<Resource> requirementVolumes = requirement.getVolumes();
    for (Resource requirementVolume : requirementVolumes) {
      String persistenceId = requirementVolume.getDisk().getPersistence().getId();
      if (!hasVolumeId(resourcesFromOffer, persistenceId)) {
        return false;
      }
    }
    return true;
  }

  private boolean hasVolumeId(List<Resource> resources, String persistenceId) {
    for (Resource resource : resources) {
      if ("disk".equals(resource.getName()) && resource.hasDisk()) {
        final DiskInfo diskInfo = resource.getDisk();
        final Persistence persistence = diskInfo.getPersistence();
        if (diskInfo.hasPersistence()
                && persistence.hasId()
                && persistenceId.equals(persistence.getId())) {
          log.info("Found matching volume with persistence id: " + persistenceId);
          return true;
        } else {
          log.info("Disk has no persistence object OR the persistenceId didn't match.");
        }
      }
    }

    log.error("Unable to find a volume with persistence id: " + persistenceId);
    return false;
  }

  private List<Offer> getAcceptablePlacementConstraintsOffers(List<Offer> offers) {
    if (offers.size() <= 0) {
      log.warn("No offers were provided to check for placement constraints.");
      return offers;
    }

    List<Offer> acceptableOffers = getColocatedOffers(offers);
    acceptableOffers = getDisjointOffers(acceptableOffers);

    if (acceptableOffers.size() > 0) {
      for (Offer offer : acceptableOffers) {
        log.info("Found Offer meeting placement constraints: " + offer);
      }
    } else {
      log.warn("No Offers found meeting placement constraints.");
    }

    return acceptableOffers;
  }

  private List<Offer> getColocatedOffers(List<Offer> offers) {
    Collection<SlaveID> colocateAgents = requirement.getColocateAgents();

    if (colocateAgents == null) {
      return offers;
    }

    List<Offer> colocatedOffers = new ArrayList<Offer>();

    for (Offer offer : offers) {
      if (colocateAgents.contains(offer.getSlaveId())) {
        colocatedOffers.add(offer);
      }
    }
    return colocatedOffers;
  }

  private List<Offer> getDisjointOffers(List<Offer> offers) {
    Collection<SlaveID> avoidAgents = requirement.getAvoidAgents();
    if (avoidAgents == null) {
      return offers;
    }

    List<Offer> disjointOffers = new ArrayList<Offer>();

    for (Offer offer : offers) {
      if (!avoidAgents.contains(offer.getSlaveId())) {
        disjointOffers.add(offer);
      }
    }

    return disjointOffers;
  }

  private boolean enoughCpu(Offer offer) {
    List<Resource> resources = offer.getResourcesList();
    double unreservedCpu = ResourceUtils.getUnreservedCpu(resources);
    double neededReservedCpu = getNeededReservedCpu(offer);
    double remainingUnreservedCpu = unreservedCpu - neededReservedCpu;

    return remainingUnreservedCpu >= requirement.getNeededUnreservedCpu();
  }

  private boolean enoughMem(Offer offer) {
    List<Resource> resources = offer.getResourcesList();
    double unreservedMem = ResourceUtils.getUnreservedMem(resources);
    double neededReservedMem = getNeededReservedMem(offer);
    double remainingUnreservedMem = unreservedMem - neededReservedMem;

    return remainingUnreservedMem >= requirement.getNeededUnreservedMem();
  }

  private boolean enoughDisk(Offer offer) {
    final VolumeRequirement volumeRequirement = requirement.getVolumeRequirement();
    final VolumeRequirement.VolumeType volumeType = volumeRequirement.getVolumeType();
    final List<Resource> resources = offer.getResourcesList();

    boolean hasEnoughDisk = false;

    if (volumeType == VolumeRequirement.VolumeType.ROOT) {
      Optional<Resource> rootDisk = ResourceUtils.filterDiskByVolumeType(resources, volumeType)
        .stream()
        .findFirst();

      if (!rootDisk.isPresent()) {
        log.error("Expecting a ROOT disk to be part of offer, and ROOT disk is not found.");
        return false;
      }
      final double neededReservedDisk = getNeededReservedDisk(Arrays.asList(rootDisk.get()));
      double unreservedDisk = ResourceUtils.getUnreservedDisk(Arrays.asList(rootDisk.get()));
      double remainingUnreservedDisk = unreservedDisk - neededReservedDisk;

      hasEnoughDisk = remainingUnreservedDisk >= requirement.getNeededUnreservedDisk();
    } else if (volumeType == VolumeRequirement.VolumeType.MOUNT) {
      List<Resource> mountDisk = ResourceUtils.filterDiskByVolumeType(resources, volumeType);
      if (mountDisk.isEmpty()) {
        log.error("Expecting a MOUNT disk to be part of offer, and disk not found.");
        return false;
      }
      final double neededReservedDisk = getNeededReservedDisk(mountDisk);
      final double unreservedDisk = ResourceUtils.getUnreservedDisk(mountDisk);
      final double totalReservedDisk =
        ResourceUtils.getTotalReservedDisk(mountDisk, requirement.getRole(), requirement.getPrincipal());

      final boolean hasEnoughDiskToBeReserved = unreservedDisk >= neededReservedDisk;
      final boolean hasEnoughReservedDisk = totalReservedDisk >= neededReservedDisk;

      hasEnoughDisk = hasEnoughDiskToBeReserved || hasEnoughReservedDisk;

      log.info("hasEnoughDiskToBeReserved: " + hasEnoughDiskToBeReserved
              + " hasEnoughReservedDisk: " + hasEnoughReservedDisk
              + " hasEnoughDisk: " + hasEnoughDisk);
    } else if (volumeType == VolumeRequirement.VolumeType.PATH) {
      log.error("VolumeType.PATH is not supported in this implementation");
      hasEnoughDisk = false;
    }

    return hasEnoughDisk;
  }

  private boolean enoughPorts(Offer offer) {
    final List<Resource> resources = offer.getResourcesList();
    List<Range> neededUnreservedPorts = requirement.getNeededUnreservedPorts();
    List<Range> neededReservedPorts = requirement.getNeededReservedPorts();

    final List<Range> neededPorts = Algorithms.mergeRanges(neededReservedPorts, neededUnreservedPorts);

    List<Range> availableUnreservedPorts = ResourceUtils.getUnreservedPorts(resources);
    List<Range> availableReservedPorts =
      ResourceUtils.getReservedPorts(resources, requirement.getRole(), requirement.getPrincipal());

    final List<Range> availablePorts = Algorithms.mergeRanges(availableUnreservedPorts, availableReservedPorts);

    return portsAvailable(availablePorts, neededPorts);
  }

  private boolean portsAvailable(List<Range> availablePorts, List<Range> neededPorts) {
    return Algorithms.subtractRanges(neededPorts, availablePorts).isEmpty();
  }

  private double getNeededReservedCpu(Offer offer) {
    List<Resource> resources = offer.getResourcesList();
    double reservedCpu = ResourceUtils.getReservedCpu(resources, requirement.getRole(), requirement.getPrincipal());
    double neededReservedCpu = requirement.getNeededReservedCpu();
    return neededReservedCpu - reservedCpu;
  }

  private double getNeededReservedMem(Offer offer) {
    List<Resource> resources = offer.getResourcesList();
    double reservedMem = ResourceUtils.getReservedMem(resources, requirement.getRole(), requirement.getPrincipal());
    double neededReservedMem = requirement.getNeededReservedMem();
    return neededReservedMem - reservedMem;
  }

  private double getNeededReservedDisk(Offer offer) {
    return getNeededReservedDisk(offer.getResourcesList());
  }

  private double getNeededReservedDisk(List<Resource> resources) {
    double reservedDisk = ResourceUtils.getAvailableReservedDisk(
      resources,
      requirement.getRole(),
      requirement.getPrincipal());
    double neededReservedDisk = requirement.getNeededReservedDisk();
    return neededReservedDisk - reservedDisk;
  }

  private List<Range> getNeededReservedPorts(Offer offer) {
    List<Resource> resources = offer.getResourcesList();
    List<Range> reservedPorts =
      ResourceUtils.getReservedPorts(resources, requirement.getRole(), requirement.getPrincipal());
    List<Range> neededReservedPorts = requirement.getNeededReservedPorts();
    return Algorithms.subtractRanges(neededReservedPorts, reservedPorts);
  }
}
