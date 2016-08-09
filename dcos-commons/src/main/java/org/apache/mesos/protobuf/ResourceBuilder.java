package org.apache.mesos.protobuf;

import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Resource.ReservationInfo;
import org.apache.mesos.Protos.Value;
import org.apache.mesos.Protos.Value.Range;

import java.util.List;

/**
 * Builder class for working with protobufs.  It includes 2 different approaches;
 * 1) static functions useful for developers that want helpful protobuf functions for Resource.
 * 2) builder class
 * All builder classes provide access to the protobuf builder for capabilities beyond the included
 * helpful functions.
 * <p/>
 * This builds Resource objects and provides some convenience functions for common resources.
 **/

public class ResourceBuilder {
  private String role;
  static final String DEFAULT_ROLE = "*";

  public ResourceBuilder(String role) {
    this.role = role;
  }

  public ResourceBuilder() {
    this(DEFAULT_ROLE);
  }

  public Resource createCpuResource(double value) {
    return cpus(value, role);
  }

  public Resource createMemResource(double value) {
    return mem(value, role);
  }

  public Resource createDiskResource(double value) {
    return disk(value, role);
  }

  public Resource createPortResource(long begin, long end) {
    return ports(begin, end, role);
  }

  public Resource createScalarResource(String name, double value) {
    return ResourceBuilder.createScalarResource(name, value, role);
  }

  public Resource createRangeResource(String name, long begin, long end) {
    return ResourceBuilder.createRangeResource(name, begin, end, role);
  }

  public static Resource createScalarResource(String name, double value, String role) {
    return Resource.newBuilder()
      .setName(name)
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder().setValue(value).build())
      .setRole(role)
      .build();
  }

  public static Resource createRangeResource(String name, long begin, long end, String role) {
    Value.Range range = Value.Range.newBuilder().setBegin(begin).setEnd(end).build();
    return Resource.newBuilder()
      .setName(name)
      .setType(Value.Type.RANGES)
      .setRanges(Value.Ranges.newBuilder().addRange(range))
      .setRole(role)
      .build();
  }

  public static Resource createRangeResource(String name, List<Range> ranges, String role) {
    return Resource.newBuilder()
      .setName(name)
      .setType(Value.Type.RANGES)
      .setRanges(Value.Ranges.newBuilder().addAllRange(ranges))
      .setRole(role)
      .build();
  }

  public static Resource addDiskInfo(Resource diskRes, Resource.DiskInfo diskInfo) {
    return Resource.newBuilder(diskRes)
      .setDisk(diskInfo)
      .build();
  }

  public static Resource reservedCpus(double value, String role, String principal) {
    Resource cpuRes = cpus(value, role);
    return addReservation(cpuRes, role, principal);
  }

  public static Resource cpus(double value) {
    return cpus(value, DEFAULT_ROLE);
  }

  public static Resource cpus(double value, String role) {
    return createScalarResource("cpus", value, role);
  }

  public static Resource reservedMem(double value, String role, String principal) {
    Resource memRes = mem(value, role);
    return addReservation(memRes, role, principal);
  }

  public static Resource mem(double value) {
    return mem(value, DEFAULT_ROLE);
  }

  public static Resource mem(double value, String role) {
    return createScalarResource("mem", value, role);
  }

  public static Resource reservedDisk(double value, String role, String principal) {
    return reservedDisk(value, role, principal, null);
  }

  public static Resource reservedDisk(double value, String role, String principal, Resource.DiskInfo diskInfo) {
    Resource diskRes = disk(value, role);
    if (diskInfo != null) {
      diskRes = addDiskInfo(diskRes, diskInfo);
    }
    return addReservation(diskRes, role, principal);
  }

  public static Resource reservedPorts(long begin, long end, String role, String principal) {
    Resource portsRes = ports(begin, end, role);
    return addReservation(portsRes, role, principal);
  }

  public static Resource reservedPorts(List<Range> ports, String role, String principal) {
    Resource portRes = ports(ports, role);
    return addReservation(portRes, role, principal);
  }

  public static Resource disk(double sizeInMB) {
    return disk(sizeInMB, DEFAULT_ROLE);
  }

  public static Resource disk(double sizeInMB, String role) {
    return createScalarResource("disk", sizeInMB, role);
  }

  public static Resource ports(long begin, long end, String role) {
    return createRangeResource("ports", begin, end, role);
  }

  public static Resource ports(long begin, long end) {
    return ports(begin, end, DEFAULT_ROLE);
  }

  public static Resource ports(List<Range> ports, String role) {
    return createRangeResource("ports", ports, role);
  }

  public static Resource addReservation(Resource resource, String role, String principal) {
    return Resource.newBuilder(resource)
      .setReservation(ReservationInfo.newBuilder()
        .setPrincipal(principal)).build();
  }
}
