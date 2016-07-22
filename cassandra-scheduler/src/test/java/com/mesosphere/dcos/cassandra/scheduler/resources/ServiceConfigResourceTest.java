package com.mesosphere.dcos.cassandra.scheduler.resources;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.io.Resources;
import com.mesosphere.dcos.cassandra.scheduler.TestUtils;
import com.mesosphere.dcos.cassandra.scheduler.config.*;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.FileConfigurationSourceProvider;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.testing.junit.ResourceTestRule;
import io.dropwizard.validation.BaseValidator;
import org.apache.curator.test.TestingServer;
import org.apache.mesos.config.ConfigStoreException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ServiceConfigResourceTest {
    private static TestingServer server;
    private static MutableSchedulerConfiguration config;
    private static ConfigurationManager configurationManager;

    @Rule
    public final ResourceTestRule resources = ResourceTestRule.builder()
            .addResource(new ServiceConfigResource(configurationManager)).build();

    @BeforeClass
    public static void beforeAll() throws Exception {

        server = new TestingServer();

        server.start();

        final ConfigurationFactory<MutableSchedulerConfiguration> factory =
                new ConfigurationFactory<>(
                        MutableSchedulerConfiguration.class,
                        BaseValidator.newValidator(),
                        Jackson.newObjectMapper().registerModule(
                                new GuavaModule())
                                .registerModule(new Jdk8Module()),
                        "dw");

        config = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                Resources.getResource("scheduler.yml").getFile());

        final CassandraSchedulerConfiguration configuration = config.createConfig();
        try {
            final DefaultConfigurationManager defaultConfigurationManager =
                    TestUtils.createConfigManager(server.getConnectString(), configuration);
            configurationManager = new ConfigurationManager(defaultConfigurationManager);
        } catch (ConfigStoreException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void afterAll() throws Exception {
        server.close();
        server.stop();
    }

    @Test
    public void testGetIdentity() throws Exception {
        ServiceConfig serviceConfig = resources.client().target("/v1/framework").request()
                .get(ServiceConfig.class);
        System.out.println("serviceConfig = " + serviceConfig);
        assertEquals(config.getServiceConfig(), serviceConfig);
    }
}
