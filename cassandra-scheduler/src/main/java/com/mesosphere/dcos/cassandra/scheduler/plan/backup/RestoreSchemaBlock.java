package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraDaemonTask;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraTask;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreSchemaTask;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.scheduler.plan.AbstractClusterTaskBlock;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.apache.mesos.scheduler.plan.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class RestoreSchemaBlock extends AbstractClusterTaskBlock<RestoreContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(
            RestoreSchemaBlock.class);

    public static RestoreSchemaBlock create(
            String daemon,
            CassandraTasks cassandraTasks,
            CassandraOfferRequirementProvider provider,
            RestoreContext context) {
        return new RestoreSchemaBlock(daemon, cassandraTasks, provider,
                context);
    }

    @Override
    protected Optional<CassandraTask> getOrCreateTask(RestoreContext context)
            throws PersistenceException {

        CassandraDaemonTask daemonTask =
                cassandraTasks.getDaemons().get(daemon);
        if (daemonTask == null) {
            LOGGER.warn("Cassandra Daemon for restore schema does not exist");
            setStatus(Status.Complete);
            return Optional.empty();
        }
        return Optional.of(cassandraTasks.getOrCreateRestoreSchema(
                daemonTask,
                context));

    }

    public RestoreSchemaBlock(
            String daemon,
            CassandraTasks cassandraTasks,
            CassandraOfferRequirementProvider provider,
            RestoreContext context) {
        super(daemon, cassandraTasks, provider, context);
    }


    @Override
    public String getName() {
        return RestoreSchemaTask.nameForDaemon(daemon);
    }
}
