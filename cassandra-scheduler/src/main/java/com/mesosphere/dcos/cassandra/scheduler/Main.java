package com.mesosphere.dcos.cassandra.scheduler;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.mesosphere.dcos.cassandra.scheduler.config.ConfigurationManager;
import com.mesosphere.dcos.cassandra.scheduler.config.CuratorFrameworkConfig;
import com.mesosphere.dcos.cassandra.scheduler.config.MutableSchedulerConfiguration;
import com.mesosphere.dcos.cassandra.scheduler.config.YAMLConfigurationFactory;
import com.mesosphere.dcos.cassandra.scheduler.health.ReconciledCheck;
import com.mesosphere.dcos.cassandra.scheduler.health.RegisteredCheck;
import com.mesosphere.dcos.cassandra.scheduler.health.ServersCheck;
import com.mesosphere.dcos.cassandra.scheduler.resources.*;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;

import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableLookup;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.java8.Java8Bundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import java.nio.charset.StandardCharsets;

import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.mesos.config.ConfigStore;
import org.apache.mesos.config.Configuration;
import org.apache.mesos.config.api.ConfigResource;
import org.apache.mesos.curator.CuratorConfigStore;
import org.apache.mesos.curator.CuratorStateStore;
import org.apache.mesos.scheduler.plan.api.StageResource;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.state.StateStoreException;
import org.apache.mesos.state.api.PropertyDeserializer;
import org.apache.mesos.state.api.StateResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main extends Application<MutableSchedulerConfiguration> {

  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws Exception {
    new Main().run(args);
  }

  public Main() {
    super();
  }

  @Override
  public String getName() {
    return "DCOS Cassandra Service";
  }

  @Override
  public void initialize(Bootstrap<MutableSchedulerConfiguration> bootstrap) {
    super.initialize(bootstrap);

    StrSubstitutor strSubstitutor = new StrSubstitutor(new EnvironmentVariableLookup(false));
    strSubstitutor.setEnableSubstitutionInVariables(true);

    bootstrap.addBundle(new Java8Bundle());
    bootstrap.setConfigurationSourceProvider(
      new SubstitutingSourceProvider(
        bootstrap.getConfigurationSourceProvider(),
        strSubstitutor));
  }

  @Override
  public void run(MutableSchedulerConfiguration configuration,
                  Environment environment) throws Exception {


    logConfiguration(configuration);

    final CuratorFrameworkConfig curatorConfig = configuration.getCuratorConfig();

    final RetryPolicy retryPolicy;
    if (curatorConfig.getOperationTimeout().isPresent()) {
      retryPolicy = new RetryUntilElapsed(
          curatorConfig.getOperationTimeoutMs().get().intValue(),
          (int) curatorConfig.getBackoffMs());
    } else {
      retryPolicy = new RetryForever((int) curatorConfig.getBackoffMs());
    }

    ConfigStore<Configuration> configStore = new CuratorConfigStore<>(
            configuration.getServiceConfig().getName(),
            curatorConfig.getServers(),
            retryPolicy);
    StateStore stateStore = new CuratorStateStore(
            configuration.getServiceConfig().getName(),
            curatorConfig.getServers(),
            retryPolicy);

    final SchedulerModule baseModule = new SchedulerModule(
      configuration.createConfig(),
      configuration.getMesosConfig(),
      environment,
      configStore,
      stateStore);

    Injector injector = Guice.createInjector(baseModule);

    registerManagedObjects(environment, injector);
    registerJerseyResources(environment, injector, configStore, stateStore);
    registerHealthChecks(environment, injector);
  }

  private void registerJerseyResources(
      Environment environment,
      Injector injector,
      ConfigStore<Configuration> configStore,
      StateStore stateStore) {
    environment.jersey().register(
      injector.getInstance(ServiceConfigResource.class));
    environment.jersey().register(
      injector.getInstance(SeedsResource.class));
    environment.jersey().register(
      injector.getInstance(ConfigurationResource.class));
    environment.jersey().register(
      injector.getInstance(TasksResource.class));
    environment.jersey().register(
      injector.getInstance(BackupResource.class));
    environment.jersey().register(
      injector.getInstance(RestoreResource.class));
    environment.jersey().register(
      injector.getInstance(CleanupResource.class));
    environment.jersey().register(
      injector.getInstance(RepairResource.class));
    environment.jersey().register(
      injector.getInstance(DataCenterResource.class));

    // APIs from dcos-commons:
    environment.jersey().register(
      injector.getInstance(StageResource.class));
    environment.jersey().register(
      new ConfigResource<>(configStore, new YAMLConfigurationFactory(getConfigurationClass())));
    environment.jersey().register(
      new StateResource(stateStore, new PropertyDeserializer() {
        @Override
        public String toJsonString(String key, byte[] value) throws StateStoreException {
            // all property data is stored as json strings
            return new String(value, StandardCharsets.UTF_8);
        }
      }));
  }

  private void registerManagedObjects(Environment environment, Injector injector) {
    environment.lifecycle().manage(
      injector.getInstance(ConfigurationManager.class));
    environment.lifecycle().manage(
      injector.getInstance(CassandraTasks.class));
    environment.lifecycle().manage(
      injector.getInstance(CassandraScheduler.class));
  }

  private void registerHealthChecks(Environment environment,
                                    Injector injector) {
    environment.healthChecks().register(RegisteredCheck.NAME,
      injector.getInstance(RegisteredCheck.class));
    environment.healthChecks().register(ServersCheck.NAME,
      injector.getInstance(ServersCheck.class));
    environment.healthChecks().register(ReconciledCheck.NAME,
      injector.getInstance(ReconciledCheck.class));
  }


  private void logConfiguration(MutableSchedulerConfiguration config) {

    LOGGER.info("Framework ServiceConfig = {}",
      config.getServiceConfig());
    LOGGER.info("Framework Mesos Configuration = {}",
      config.getMesosConfig());
    LOGGER.info("Framework ZooKeeper Configuration = {}",
      config.getCuratorConfig());
    LOGGER.info(
      "------------ Cassandra Configuration ------------");
    LOGGER.info("heap = {}", config.getCassandraConfig().getHeap());
    LOGGER.info("jmx port = {}", config.getCassandraConfig()
      .getJmxPort());
    LOGGER.info("location = {}", config.getCassandraConfig()
      .getLocation());
    config
      .getCassandraConfig()
      .getApplication()
      .toMap()
      .forEach((key, value) -> LOGGER.info("{} = {}", key, value));
  }

}
