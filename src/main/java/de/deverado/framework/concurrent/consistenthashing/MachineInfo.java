package de.deverado.framework.concurrent.consistenthashing;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

/**
 * Must implement equals and toString by id.
 */
public interface MachineInfo {

        public String getId();

        /**
         * Depending on ring this might be a sum of led vnodes and replica vnodes or only led vnodes.
         */
        public int getVnodeCount();

        public MachineInRingState getState();
}
