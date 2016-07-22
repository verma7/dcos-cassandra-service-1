package com.mesosphere.dcos.cassandra.scheduler.config;


import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.io.Resources;
import com.mesosphere.dcos.cassandra.common.config.ExecutorConfig;
import com.mesosphere.dcos.cassandra.scheduler.TestUtils;

import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.FileConfigurationSourceProvider;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.validation.BaseValidator;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class ConfigurationManagerTest {
    private TestingServer server;
    private ConfigurationFactory<MutableSchedulerConfiguration> factory;

    @Before
    public void beforeAll() throws Exception {
        server = new TestingServer();
        server.start();
        factory = new ConfigurationFactory<>(
                        MutableSchedulerConfiguration.class,
                        BaseValidator.newValidator(),
                        Jackson.newObjectMapper()
                                .registerModule(new GuavaModule())
                                .registerModule(new Jdk8Module()),
                        "dw");
    }

    @Test
    public void loadAndPersistConfiguration() throws Exception {
        final String configFilePath = Resources.getResource("scheduler.yml").getFile();
        MutableSchedulerConfiguration mutableConfig = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                configFilePath);
        final CassandraSchedulerConfiguration original  = mutableConfig.createConfig();
        DefaultConfigurationManager configurationManager =
                TestUtils.createConfigManager(server.getConnectString(), original);
        ConfigurationManager manager = new ConfigurationManager(configurationManager);
        CassandraSchedulerConfiguration targetConfig = (CassandraSchedulerConfiguration)configurationManager.getTargetConfig();
        assertEquals("cassandra", original.getServiceConfig().getName());
        assertEquals("cassandra-role", original.getServiceConfig().getRole());
        assertEquals("cassandra-cluster", original.getServiceConfig().getCluster());
        assertEquals("cassandra-principal",
                original.getServiceConfig().getPrincipal());
        assertEquals("", original.getServiceConfig().getSecret());

        manager.start();

        assertEquals(original.getCassandraConfig(), targetConfig.getCassandraConfig());
        assertEquals(original.getExecutorConfig(), targetConfig.getExecutorConfig());
        assertEquals(original.getServers(), targetConfig.getServers());
        assertEquals(original.getSeeds(), targetConfig.getSeeds());
    }


    @Test
    public void applyConfigUpdate() throws Exception {
        MutableSchedulerConfiguration mutable = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                Resources.getResource("scheduler.yml").getFile());
        final CassandraSchedulerConfiguration original = mutable.createConfig();
        DefaultConfigurationManager configurationManager =
                TestUtils.createConfigManager(server.getConnectString(), original);
        ConfigurationManager manager = new ConfigurationManager(configurationManager);
        CassandraSchedulerConfiguration targetConfig =
          (CassandraSchedulerConfiguration)configurationManager.getTargetConfig();

        manager.start();

        assertEquals(original.getCassandraConfig(), targetConfig.getCassandraConfig());
        assertEquals(original.getExecutorConfig(), targetConfig.getExecutorConfig());
        assertEquals(original.getServers(), targetConfig.getServers());
        assertEquals(original.getSeeds(), targetConfig.getSeeds());

        manager.stop();

        ExecutorConfig updatedExecutorConfig = new ExecutorConfig(
                "/command/line",
                new ArrayList<>(),
                1.2,
                345,
                901,
                17,
                "/java/home",
                URI.create("/jre/location"), URI.create("/executor/location"),
                URI.create("/cassandra/location"),
                "unlimited",
                "100000",
                "32768");
        int updatedServers = original.getServers() + 10;
        int updatedSeeds = original.getSeeds() + 5;

        mutable = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                Resources.getResource("scheduler.yml").getFile());


        mutable.setSeeds(updatedSeeds);
        mutable.setServers(updatedServers);
        mutable.setExecutorConfig(updatedExecutorConfig);

        mutable.setCassandraConfig(
          mutable.getCassandraConfig()
        .mutable().setJmxPort(8000).setCpus(0.6).setMemoryMb(10000).build());

        CassandraSchedulerConfiguration updated = mutable.createConfig();

        configurationManager = TestUtils.createConfigManager(server.getConnectString(), updated);
        configurationManager.store(updated);
        manager = new ConfigurationManager(configurationManager);
        targetConfig = (CassandraSchedulerConfiguration)configurationManager.getTargetConfig();

        manager.start();

        assertEquals(updated.getCassandraConfig(), targetConfig.getCassandraConfig());

        assertEquals(updatedExecutorConfig, targetConfig.getExecutorConfig());

        assertEquals(updatedServers, targetConfig.getServers());

        assertEquals(updatedSeeds, targetConfig.getSeeds());
    }

    @Test
    public void failOnBadServersCount() throws Exception {
        MutableSchedulerConfiguration mutable = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                Resources.getResource("scheduler.yml").getFile());
        CassandraSchedulerConfiguration originalConfig = mutable.createConfig();

        DefaultConfigurationManager configurationManager =
                TestUtils.createConfigManager(server.getConnectString(), originalConfig);
        ConfigurationManager manager = new ConfigurationManager(configurationManager);
        CassandraSchedulerConfiguration targetConfig = (CassandraSchedulerConfiguration)configurationManager.getTargetConfig();

        manager.start();

        assertEquals(originalConfig.getCassandraConfig(), targetConfig.getCassandraConfig());
        assertEquals(originalConfig.getExecutorConfig(), targetConfig.getExecutorConfig());
        assertEquals(originalConfig.getServers(), targetConfig.getServers());
        assertEquals(originalConfig.getSeeds(), targetConfig.getSeeds());

        manager.stop();

        int updatedServers = originalConfig.getServers() - 1;
        mutable.setServers(updatedServers);

        configurationManager =
                TestUtils.createConfigManager(server.getConnectString(), mutable.createConfig());
        manager = new ConfigurationManager(configurationManager);

        manager.start();

        assertEquals(1, configurationManager.getErrors().size());
    }

    @Test
    public void failOnBadSeedsCount() throws Exception {
        MutableSchedulerConfiguration mutableSchedulerConfiguration = factory.build(
                new SubstitutingSourceProvider(
                        new FileConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false, true)),
                Resources.getResource("scheduler.yml").getFile());
        CassandraSchedulerConfiguration originalConfig = mutableSchedulerConfiguration.createConfig();
        DefaultConfigurationManager configurationManager =
                TestUtils.createConfigManager(server.getConnectString(), originalConfig);
        ConfigurationManager manager = new ConfigurationManager(configurationManager);
        CassandraSchedulerConfiguration targetConfig = (CassandraSchedulerConfiguration)configurationManager.getTargetConfig();

        manager.start();

        assertEquals(originalConfig.getCassandraConfig(), targetConfig.getCassandraConfig());
        assertEquals(originalConfig.getExecutorConfig(), targetConfig.getExecutorConfig());
        assertEquals(originalConfig.getServers(), targetConfig.getServers());
        assertEquals(originalConfig.getSeeds(), targetConfig.getSeeds());

        manager.stop();

        int updatedSeeds = originalConfig.getServers() + 1;
        mutableSchedulerConfiguration.setSeeds(updatedSeeds);

        configurationManager = TestUtils.createConfigManager(
                server.getConnectString(), mutableSchedulerConfiguration.createConfig());
        manager = new ConfigurationManager(configurationManager);
        manager.start();
        assertEquals(1, configurationManager.getErrors().size());
    }

    @After
    public void afterAll() throws Exception {
        server.close();
        server.stop();
    }
}
