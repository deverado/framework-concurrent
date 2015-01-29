package de.deverado.framework.concurrent.consistenthashing;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public interface VNodeConsistentHashRingChangeListener {

    public void changed(@Nullable VNodeConsistentHashRing old, VNodeConsistentHashRing newRing,
                        RingDifference difference);
}
