package de.deverado.framework.concurrent.consistenthashing;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class RingDifferenceImpl implements RingDifference {


    private Set<MachineInfo> changed;

    public static RingDifferenceImpl create(@Nullable VNodeConsistentHashRing currentRing,
                                            Collection<? extends MachineInfo> newInfos) {

        List<MachineInfo> oldMachines = currentRing == null ?
                Collections.<MachineInfo>emptyList() : currentRing.getMachineInfos();
        Map<String, MachineInfo> newMachines = makeMap(newInfos);

        Set<MachineInfo> changed = new HashSet<>();
        for (MachineInfo old : oldMachines) {
            MachineInfo newMachineInfo = newMachines.get(old.getId());
            if (newMachineInfo == null) {
                // removed
                changed.add(old);
            } else if (!Objects.equals(old.getState(), newMachineInfo.getState())) {
                // state changed
                changed.add(newMachineInfo);
            }
            newMachines.remove(old.getId());
        }
        // machines that are new in new:
        changed.addAll(newMachines.values());

        RingDifferenceImpl result = new RingDifferenceImpl();
        result.changed = changed;
        return result;
    }

    private static Map<String, MachineInfo> makeMap(Collection<? extends MachineInfo> machineInfos) {

        Map<String, MachineInfo> result = new HashMap<>(machineInfos.size());
        for (MachineInfo i : machineInfos) {
            result.put(i.getId(), i);
        }
        return result;
    }

    @Override
    public boolean isHavingDifference() {
        return !changed.isEmpty();
    }

    @Override
    public Set<MachineInfo> getChanged() {
        return changed;
    }
}
