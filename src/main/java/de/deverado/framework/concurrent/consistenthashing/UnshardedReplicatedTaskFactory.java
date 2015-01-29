package de.deverado.framework.concurrent.consistenthashing;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Creates {@link de.deverado.framework.concurrent.consistenthashing.UnshardedReplicatedTask} based on a name.
 * @see de.deverado.framework.concurrent.consistenthashing.FixedShardCountReplicatedTaskMgr
 */
@ParametersAreNonnullByDefault
public interface UnshardedReplicatedTaskFactory {

    public UnshardedReplicatedTask create(String name);

}
