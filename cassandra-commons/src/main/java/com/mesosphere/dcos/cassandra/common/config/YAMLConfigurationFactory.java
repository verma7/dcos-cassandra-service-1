package com.mesosphere.dcos.cassandra.common.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.dropwizard.jackson.Jackson;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.config.Configuration;
import org.apache.mesos.config.ConfigurationFactory;

public class YAMLConfigurationFactory implements ConfigurationFactory<Configuration> {
    private Class<?> typeParameterClass;

    public YAMLConfigurationFactory(Class<?> typeParameterClass) {
        this.typeParameterClass = typeParameterClass;
    }

    @Override
    public Configuration parse(byte[] bytes) throws ConfigStoreException {
        try {
            final ObjectMapper mapper = Jackson.newObjectMapper()
                    .registerModule(new GuavaModule())
                    .registerModule(new JavaTimeModule())
                    .registerModule(new Jdk8Module())
                    // Do not fail if we encounter unknown properties so that we can safely roll back
                    // the scheduler configuration which may not have the new field values.
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            return (Configuration) mapper.readValue(bytes, typeParameterClass);
        } catch (Exception e) {
            throw new ConfigStoreException(e);
        }
    }
}
