package de.deverado.framework.concurrent.consistenthashing;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

import java.util.Objects;

public enum AssignmentState {

    READ_REPLICA,

    WRITE_REPLICA,

    READ_WRITE_REPLICA,

    UNRELATED,

    ;

    public boolean isUnrelatedState() {
        return Objects.equals(this, UNRELATED);
    }

    public boolean isReplicaState() {
        switch (this) {
            case UNRELATED:
                return false;
            case WRITE_REPLICA:
            case READ_WRITE_REPLICA:
            case READ_REPLICA:
                return true;
            default:
                throw new IllegalStateException("Missing a state: " + getClass());
        }
    }
}
