package org.apache.mesos.offer;

import org.apache.mesos.Protos;
import org.apache.mesos.protobuf.ResourceBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class ResourceUtilsTest {
    @Test
    public void testFilterDiskByVolumeTypeROOT() {
        final String role = "m_role";
        final String principal = "m_principal";
        Protos.Offer offer = createOffer(
                UUID.randomUUID().toString(),
                "slave",
                "yoyo",
                "localhost",
                Arrays.asList(
                        ResourceBuilder.reservedCpus(
                                1,
                                role,
                                principal),
                        ResourceBuilder.reservedMem(
                                1,
                                role,
                                principal),
                        ResourceBuilder.reservedDisk(
                                1,
                                role,
                                principal)
                )
        );

        Assert.assertNotNull(ResourceUtils.filterDiskByVolumeType(offer.getResourcesList(), VolumeRequirement.VolumeType.ROOT));

        offer = createOffer(
                UUID.randomUUID().toString(),
                "slave",
                "yoyo",
                "localhost",
                Arrays.asList(
                        ResourceBuilder.reservedCpus(
                                1,
                                role,
                                principal),
                        ResourceBuilder.reservedMem(
                                1,
                                role,
                                principal),
                        ResourceBuilder.reservedDisk(
                                1,
                                role,
                                principal)
                )
        );

        Assert.assertNotNull(ResourceUtils.filterDiskByVolumeType(offer.getResourcesList(), VolumeRequirement.VolumeType.ROOT));

        Protos.Resource reservedDisk = ResourceBuilder.reservedDisk(
                1,
                role,
                principal);

        reservedDisk = Protos.Resource.newBuilder(reservedDisk).setDisk(
                Protos.Resource.DiskInfo.newBuilder().build()
        ).build();
        offer = createOffer(
                UUID.randomUUID().toString(),
                "slave",
                "yoyo",
                "localhost",
                Arrays.asList(
                        ResourceBuilder.reservedCpus(
                                1,
                                role,
                                principal),
                        ResourceBuilder.reservedMem(
                                1,
                                role,
                                principal),
                        reservedDisk
                )
        );

        Assert.assertNotNull(ResourceUtils.filterDiskByVolumeType(offer.getResourcesList(), VolumeRequirement.VolumeType.ROOT));
    }

    @Test
    public void testFilterDiskByVolumeTypeMOUNT() {
        final String role = "m_role";
        final String principal = "m_principal";
        Protos.Offer offer = createOffer(
                UUID.randomUUID().toString(),
                "slave",
                "yoyo",
                "localhost",
                Arrays.asList(
                        ResourceBuilder.reservedCpus(
                                1,
                                role,
                                principal),
                        ResourceBuilder.reservedMem(
                                1,
                                role,
                                principal),
                        ResourceBuilder.reservedDisk(
                                1,
                                role,
                                principal,
                                Protos.Resource.DiskInfo.newBuilder()
                                        .setSource(
                                                Protos.Resource.DiskInfo.Source
                                                        .newBuilder()
                                                        .setType(Protos.Resource.DiskInfo.Source.Type.MOUNT))
                                        .build())
                )
        );

        Assert.assertNotNull(ResourceUtils.filterDiskByVolumeType(offer.getResourcesList(), VolumeRequirement.VolumeType.MOUNT));
    }

    public static Protos.Offer createOffer(String id,
                                           String slave,
                                           String framework,
                                           String hostname,
                                           List<Protos.Resource> resources) {

        return Protos.Offer.newBuilder()
                .setId(Protos.OfferID.newBuilder().setValue(id))
                .setSlaveId(Protos.SlaveID.newBuilder().setValue(slave))
                .setHostname(hostname)
                .setFrameworkId(Protos.FrameworkID.newBuilder()
                        .setValue(framework))
                .addAllResources(resources).build();
    }
}
