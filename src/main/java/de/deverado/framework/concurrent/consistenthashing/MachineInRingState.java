package de.deverado.framework.concurrent.consistenthashing;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * This state reflects status in-ring, not if the actual machine is running or available.
 */
public enum MachineInRingState {

    /**
     * Machine is joining the ring - data is transferred to machine.
     * <p>
     * Write target: yes (writes also go to current data owner).
     * Read target: no.
     * </p>
     */
    BOOTSTRAPPING,

    /**
     * Machine is in the ring and owning data.
     * <p>
     * Write target: yes.
     * Read target: yes.
     * </p>
     */
    INTEGRATED,

    /**
     * Machine is leaving the ring - data is moved from machine. Some ring builders need the ,achine to be there so that
     * its replicas can be found (Reminder: Some rings might find replicas for a node by order of machines. So machines
     * cannot just leave the ring - then also the replicas will not be found.
     * <p>
     *     Machines must be removed (go to OUT_OF_RING) if they go offline otherwise the ring will not move reads to
     *     the new replicas that were already getting the writes for the leaving machine.
     * </p>
     * <p>
     * Write target: yes.
     * Read target: yes (in combination with new data owner).
     * </p>
     */
    LEAVING,

    /**
     * After all data was replicated to new owner and new replicas machine can go into this state and finally be
     * dropped from the ring.
     */
    OUT_OF_RING,

    ;

    /**
     * TODO a bootstrapping service that is newly deployed could change the whole node back to bootstrapping,
     * taking the node down for reads on other services - potentially taking down the cluster for reads.
     * So need to deploy such services with their own bootstrapping ring until they are integrated.
     *
     */
    @Nullable
    public static MachineInRingState deriveNewState(Logger log, List<MachineInRingState> allServiceStates,
                                                    @Nullable MachineInRingState currentMachineState) {

        Set<MachineInRingState> statesSet = new HashSet<>(allServiceStates);
        if (statesSet.contains(OUT_OF_RING)) {
            return OUT_OF_RING;
        }
        if (Objects.equals(OUT_OF_RING, currentMachineState)) {
            return OUT_OF_RING;
        }

        if (statesSet.contains(LEAVING)) {
            return LEAVING;
        }
        if (Objects.equals(LEAVING, currentMachineState)) {
            return LEAVING;
        }

        if (Objects.equals(currentMachineState, INTEGRATED)) {
            if (statesSet.contains(BOOTSTRAPPING)) {
                log.error("Service found that is bootstrapping on INTEGRATED machine - not switching to " +
                        "BOOTSTRAPPING to avoid taking down node for reads. Deploy such services on their own ring " +
                        "until integrated.");
            }
            return INTEGRATED;

        } else { // machine not integrated (and not LEAVING or OUT, so null or BOOTSTRAPPING)
            if (statesSet.contains(BOOTSTRAPPING)) {
                return BOOTSTRAPPING;
            }
            if (statesSet.contains(INTEGRATED) && statesSet.size() == 1) {
                return INTEGRATED; // might be state switch to integrated!
            } else {
                if (statesSet.isEmpty()) {
                    return currentMachineState;
                }
                log.error("Weird states in statesSet: {}, checked all, but seems to hold unknown, " +
                        "check deriveNewState! Not changing machine state", statesSet);
                return currentMachineState;
            }
        }

    }
}
