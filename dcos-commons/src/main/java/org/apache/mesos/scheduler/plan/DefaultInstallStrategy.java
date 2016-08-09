package org.apache.mesos.scheduler.plan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Provides the default strategy for installations. The strategy is
 * manually interruptable, but it does not admit decision points.
 */
public class DefaultInstallStrategy implements PhaseStrategy {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    // todo:  this has almost dup code from DefaultStage.  we should make it 1
    private final Phase phase;
    private final AtomicBoolean interrupted = new AtomicBoolean(false);

    public DefaultInstallStrategy(Phase phase) {
        this.phase = phase;
    }

    @Override
    public Block getCurrentBlock() {
        if (interrupted.get()) {
            return null;
        } else {
            for (Block block : phase.getBlocks()) {
                if (!block.isComplete()) {
                    return block;
                }
            }
            return null;
        }
    }

    @Override
    public void proceed() {
        interrupted.set(false);
    }

    @Override
    public void interrupt() {
        interrupted.set(true);
    }

    @Override
    public boolean isInterrupted() {
        return getStatus() == Status.Waiting;
    }

    @Override
    public void restart(UUID blockId) {
        // NOP
    }

    @Override
    public void forceComplete(UUID blockId) {

        if (blockId == null || phase == null) {
            return;
        }

        Block block = phase.getBlock(blockId);

        if (block != null) {
            block.setStatus(Status.Complete);
        }
    }

    @Override
    public Status getStatus() {
        if (phase == null || phase.getBlocks().isEmpty()) {
            return Status.Complete;
        }

        int blockIndex = -1;
        for (Block block : phase.getBlocks()) {
            blockIndex++;

            if (block.getStatus() != Status.Complete) {
                if (interrupted.get()) {
                    return Status.Waiting;
                }

                if (blockIndex > 0) {
                    return Status.InProgress;
                } else {
                    return block.getStatus();
                }
            }
        }

        return Status.Complete;
    }

    @Override
    public Phase getPhase() {
        return phase;
    }

    @Override
    public boolean hasDecisionPoint(Block block) {
        return false;
    }
}
