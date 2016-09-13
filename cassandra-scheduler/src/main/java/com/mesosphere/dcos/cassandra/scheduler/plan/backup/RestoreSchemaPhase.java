package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.tasks.backup.RestoreContext;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.plan.AbstractClusterTaskPhase;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class RestoreSchemaPhase extends AbstractClusterTaskPhase<RestoreSchemaBlock, RestoreContext> {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(RestoreSchemaPhase.class);

    public RestoreSchemaPhase(
            RestoreContext context,
            CassandraTasks cassandraTasks,
            CassandraOfferRequirementProvider provider) {
        super(context, cassandraTasks, provider);
    }

    protected List<RestoreSchemaBlock> createBlocks() {
        final List<String> daemons =
                new ArrayList<>(cassandraTasks.getDaemons().keySet());
        Collections.sort(daemons);
        return daemons.stream().map(daemon -> RestoreSchemaBlock.create(
                daemon,
                cassandraTasks,
                provider,
                context
        )).collect(Collectors.toList());
    }

    @Override
    public String getName() {
        return "Restore";
    }
}
