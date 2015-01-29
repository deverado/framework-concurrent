package de.deverado.framework.concurrent.consistenthashing;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

@ParametersAreNonnullByDefault
public interface MachineInRingStateListener {

    public void changed(@Nullable MachineInRingState previous, @Nullable MachineInRingState newState);
}
