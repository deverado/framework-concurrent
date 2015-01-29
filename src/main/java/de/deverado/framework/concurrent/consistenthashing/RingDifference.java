package de.deverado.framework.concurrent.consistenthashing;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

import java.util.Set;

public interface RingDifference {

    public boolean isHavingDifference();

    /**
     * @return machines that were changed - removed, added, or which's state was changed.
     */
    public Set<MachineInfo> getChanged();

}
