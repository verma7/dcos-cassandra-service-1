package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupContext;
import com.mesosphere.dcos.cassandra.scheduler.offer.CassandraOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.scheduler.plan.AbstractClusterTaskPhase;
import com.mesosphere.dcos.cassandra.scheduler.tasks.CassandraTasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * During backup schema phase, data will be schemated across all cassandra nodes.
 */
public class BackupSchemaPhase extends AbstractClusterTaskPhase<BackupSchemaBlock, BackupContext> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(BackupSchemaPhase.class);

    public BackupSchemaPhase(
            BackupContext backupContext,
            CassandraTasks cassandraTasks,
            CassandraOfferRequirementProvider provider) {
        super(backupContext, cassandraTasks, provider);
    }

    protected List<BackupSchemaBlock> createBlocks() {
        final List<String> daemons =
                new ArrayList<>(cassandraTasks.getDaemons().keySet());
        Collections.sort(daemons);
        return daemons.stream().map(daemon -> BackupSchemaBlock.create(
                daemon,
                cassandraTasks,
                provider,
                context
        )).collect(Collectors.toList());
    }

    @Override
    public String getName() {
        return "BackupSchema";
    }
}
