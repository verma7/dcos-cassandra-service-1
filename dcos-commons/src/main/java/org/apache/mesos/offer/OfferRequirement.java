package org.apache.mesos.offer;

import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.Value.Range;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * An OfferRequirement encapsulates the needed resources an Offer must have.
 * In general these are Resource requirements like it must have a certain amount of
 * cpu, memory, and disk.  Additionally it has two modes regarding expectations around
 * Persistent Volumes.  In the CREATE mode it anticipates that the Scheduler will be
 * creating the required volume, so a Volume with a particular persistence id is not
 * required to be already present in an Offer.  In the EXISTING mode, we expect that
 * an Offer will already have the indicated persistence ID.
 */
public class OfferRequirement {
  private String role;
  private String principal;
  private Collection<TaskInfo> taskInfos;
  private int randomPortCount;
  private Collection<SlaveID> avoidAgents;
  private Collection<SlaveID> colocateAgents;
  private VolumeRequirement volumeRequirement;
  private ExecutorMode executorMode = ExecutorMode.CREATE;

  /**
   * ExecutorMode.
   */
  public enum ExecutorMode {
    EXISTING,
    CREATE
  }

  public OfferRequirement(
    String role,
    String principal,
    Collection<TaskInfo> taskInfos,
    int randomPortCount,
    Collection<SlaveID> avoidAgents,
    Collection<SlaveID> colocateAgents,
    VolumeRequirement volumeRequirement,
    ExecutorMode executorMode) {

    this.role = role;
    this.principal = principal;
    this.taskInfos = taskInfos;
    this.randomPortCount = randomPortCount;
    this.avoidAgents = avoidAgents;
    this.colocateAgents = colocateAgents;
    this.volumeRequirement = volumeRequirement;
    this.executorMode = executorMode;
  }

  public OfferRequirement(
          String role,
          String principal,
          Collection<TaskInfo> taskInfos,
          Collection<SlaveID> avoidAgents,
          Collection<SlaveID> colocateAgents,
          VolumeRequirement volumeRequirement,
          ExecutorMode executorMode) {
    this(role, principal, taskInfos, 0, avoidAgents, colocateAgents, volumeRequirement, executorMode);
  }

  public OfferRequirement(
          String role,
          String principal,
          Collection<TaskInfo> taskInfos,
          Collection<SlaveID> avoidAgents,
          Collection<SlaveID> colocateAgents,
          VolumeRequirement volumeRequirement) {
    this(role, principal, taskInfos, 0, avoidAgents, colocateAgents, volumeRequirement, ExecutorMode.CREATE);
  }

  public OfferRequirement(
    Collection<TaskInfo> taskInfos,
    int randomPortCount,
    Collection<SlaveID> avoidAgents,
    Collection<SlaveID> colocateAgents,
    VolumeRequirement volumeRequirement,
    ExecutorMode executorMode) {
    this("*", null, taskInfos, randomPortCount, avoidAgents, colocateAgents, volumeRequirement, executorMode);
  }

  public OfferRequirement(
          Collection<TaskInfo> taskInfos,
          int randomPortCount,
          Collection<SlaveID> avoidAgents,
          Collection<SlaveID> colocateAgents,
          VolumeRequirement volumeRequirement) {
    this("*", null, taskInfos, randomPortCount, avoidAgents, colocateAgents, volumeRequirement, ExecutorMode.CREATE);
  }

  public OfferRequirement(
    Collection<TaskInfo> taskInfos,
    Collection<SlaveID> avoidAgents,
    Collection<SlaveID> colocateAgents,
    VolumeRequirement volumeRequirement,
    ExecutorMode executorMode) {
    this(taskInfos, 0, avoidAgents, colocateAgents, volumeRequirement, executorMode);
  }

  public OfferRequirement(
          Collection<TaskInfo> taskInfos,
          Collection<SlaveID> avoidAgents,
          Collection<SlaveID> colocateAgents,
          VolumeRequirement volumeRequirement) {
    this(taskInfos, 0, avoidAgents, colocateAgents, volumeRequirement, ExecutorMode.CREATE);
  }

  public OfferRequirement(
    Collection<TaskInfo> taskInfos,
    Collection<SlaveID> avoidAgents,
    Collection<SlaveID> colocateAgents) {
    this(taskInfos, 0, avoidAgents, colocateAgents, VolumeRequirement.create(), ExecutorMode.CREATE);
  }

  public OfferRequirement(
    Collection<TaskInfo> taskInfos,
    Collection<SlaveID> avoidAgents) {
    this(taskInfos, avoidAgents, null);
  }


  public OfferRequirement(List<TaskInfo> taskInfos) {
    this(taskInfos, null, null, VolumeRequirement.create(), ExecutorMode.CREATE);
  }

  public String getRole() {
    return role;
  }

  public String getPrincipal() {
    return principal;
  }

  public Collection<TaskInfo> getTaskInfos() {
    return taskInfos;
  }

  public int getRandomPortCount() {
    return randomPortCount;
  }

  public Collection<SlaveID> getAvoidAgents() {
    return avoidAgents;
  }

  public Collection<SlaveID> getColocateAgents() {
    return colocateAgents;
  }

  public VolumeRequirement getVolumeRequirement() {
    return volumeRequirement;
  }

  public double getNeededUnreservedCpu() {
    double neededUnreservedCpu = 0.0;

    for (TaskInfo info : taskInfos) {
      List<Resource> resources = info.getResourcesList();
      neededUnreservedCpu += ResourceUtils.getUnreservedCpu(resources);

      ExecutorInfo execInfo = info.getExecutor();
      if (execInfo != null && this.executorMode == ExecutorMode.CREATE) {
        resources = execInfo.getResourcesList();
        neededUnreservedCpu += ResourceUtils.getUnreservedCpu(resources);
      }
    }

    return neededUnreservedCpu;
  }

  public double getNeededReservedCpu() {
    double neededReservedCpu = 0.0;

    for (TaskInfo info : taskInfos) {
      List<Resource> resources = info.getResourcesList();
      neededReservedCpu += ResourceUtils.getReservedCpu(resources, role, principal);

      ExecutorInfo execInfo = info.getExecutor();
      if (execInfo != null && this.executorMode == ExecutorMode.CREATE) {
        resources = execInfo.getResourcesList();
        neededReservedCpu += ResourceUtils.getReservedCpu(resources, role, principal);
      }
    }

    return neededReservedCpu;
  }

  public double getNeededUnreservedMem() {
    double neededUnreservedMem = 0.0;

    for (TaskInfo info : taskInfos) {
      List<Resource> resources = info.getResourcesList();
      neededUnreservedMem += ResourceUtils.getUnreservedMem(resources);

      ExecutorInfo execInfo = info.getExecutor();
      if (execInfo != null && this.executorMode == ExecutorMode.CREATE) {
        resources = execInfo.getResourcesList();
        neededUnreservedMem += ResourceUtils.getUnreservedMem(resources);
      }
    }

    return neededUnreservedMem;
  }

  public double getNeededReservedMem() {
    double neededReservedMem = 0.0;

    for (TaskInfo info : taskInfos) {
      List<Resource> resources = info.getResourcesList();
      neededReservedMem += ResourceUtils.getReservedMem(resources, role, principal);

      ExecutorInfo execInfo = info.getExecutor();
      if (execInfo != null && this.executorMode == ExecutorMode.CREATE) {
        resources = execInfo.getResourcesList();
        neededReservedMem += ResourceUtils.getReservedMem(resources, role, principal);
      }
    }

    return neededReservedMem;
  }

  public double getNeededUnreservedDisk() {
    double neededUnreservedDisk = 0.0;

    for (TaskInfo info : taskInfos) {
      List<Resource> resources = info.getResourcesList();
      neededUnreservedDisk += ResourceUtils.getUnreservedDisk(resources);

      ExecutorInfo execInfo = info.getExecutor();
      if (execInfo != null && this.executorMode == ExecutorMode.CREATE) {
        resources = execInfo.getResourcesList();
        neededUnreservedDisk += ResourceUtils.getUnreservedDisk(resources);
      }
    }

    return neededUnreservedDisk;
  }

  public double getNeededReservedDisk() {
    double neededReservedDisk = 0.0;

    final VolumeRequirement.VolumeMode volMode = volumeRequirement.getVolumeMode();

    for (TaskInfo info : taskInfos) {
      List<Resource> resources = info.getResourcesList();

      switch (volMode) {
        case CREATE:
          neededReservedDisk += ResourceUtils.getTotalReservedDisk(resources, role, principal);
          break;
        case EXISTING:
          neededReservedDisk += ResourceUtils.getAvailableReservedDisk(resources, role, principal);
          break;
        case NONE:
          neededReservedDisk += ResourceUtils.getTotalReservedDisk(resources, role, principal);
          break;
      }

      ExecutorInfo execInfo = info.getExecutor();
      if (execInfo != null && this.executorMode == ExecutorMode.CREATE) {
        resources = execInfo.getResourcesList();
        neededReservedDisk += ResourceUtils.getAvailableReservedDisk(resources, role, principal);
      }
    }

    return neededReservedDisk;
  }

  public List<Range> getNeededUnreservedPorts() {
    List<Range> portRanges = new ArrayList<Range>();

    for (TaskInfo info : taskInfos) {
      List<Resource> resources = info.getResourcesList();
      portRanges.addAll(ResourceUtils.getUnreservedPorts(resources));

      ExecutorInfo execInfo = info.getExecutor();
      if (execInfo != null && this.executorMode == ExecutorMode.CREATE) {
        resources = execInfo.getResourcesList();
        portRanges.addAll(ResourceUtils.getUnreservedPorts(resources));
      }
    }

    return portRanges;
  }


  public List<Range> getNeededReservedPorts() {
    List<Range> portRanges = new ArrayList<Range>();

    for (TaskInfo info : taskInfos) {
      List<Resource> resources = info.getResourcesList();
      portRanges.addAll(ResourceUtils.getReservedPorts(resources, role, principal));

      ExecutorInfo execInfo = info.getExecutor();
      if (execInfo != null && this.executorMode == ExecutorMode.CREATE) {
        resources = execInfo.getResourcesList();
        portRanges.addAll(ResourceUtils.getReservedPorts(resources, role, principal));
      }
    }

    return portRanges;
  }

  public List<Resource> getVolumes() {
    List<Resource> volumes = new ArrayList<Resource>();
    for (TaskInfo info : taskInfos) {
      volumes.addAll(getVolumes(info));
    }

    return volumes;
  }

  public List<Resource> getVolumes(TaskInfo taskInfo) {
    List<Resource> volumes = new ArrayList<Resource>();
    volumes.addAll(getDiskResources(taskInfo.getResourcesList()));

    ExecutorInfo execInfo = taskInfo.getExecutor();
    if (execInfo != null && this.executorMode == ExecutorMode.CREATE) {
      volumes.addAll(getDiskResources(execInfo.getResourcesList()));
    }

    return volumes;
  }

  public List<Resource> getDiskResources(List<Resource> resources) {
    List<Resource> diskResources = new ArrayList<Resource>();

    for (Resource resource : resources) {
      if (resource.getName().equals("disk")) {
        diskResources.add(resource);
      }
    }

    return diskResources;
  }
}
