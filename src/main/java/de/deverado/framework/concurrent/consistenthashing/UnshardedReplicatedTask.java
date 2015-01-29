package de.deverado.framework.concurrent.consistenthashing;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

import javax.annotation.ParametersAreNonnullByDefault;

/**
 * A task that should benefit from automatic distribution on the consistent hash cluster but is not sharded.
 * For example checking a specific resource from time to
 * time: It doesn't make sense to have more than say 3 machines checking the resource. This task and its manager
 * {@link UnshardedReplicatedTaskMgr} ensure that most of the time
 * only one replica set is running the job.
 */
@ParametersAreNonnullByDefault
public interface UnshardedReplicatedTask {

    /**
     * This name must be equal for all instances of the same task on all instances of the cluster. It is used to
     * identify the task and assign it to a replica set.
     */
    public String getName();

    /**
     * Called if the machine is assigned as a replica for the task.
     * @param newState read or write or read and write.
     */
    public void becomeReplica(AssignmentState newState);

    /**
     * Called if the machine looses its assignment to the task. Also called if the machine is shut down.
     * Shutting down and becoming unrelated cannot be differentiated clearly as the ring might change while
     * the machine is shut down, making the two states indistinguishable.
     */
    public void becomeUnrelated();

}
