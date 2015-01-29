package de.deverado.framework.concurrent.consistenthashing;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * A target for sharding. Could be a resource like a disk, but for understandability just called 'machine'.
 */
@ParametersAreNonnullByDefault
public class Machine {

    private String machineId;
    private MachineInRingState state;
    private List<Machine> allMachineList;
    private int idxInAllMachineList;
    private List<VNodeImpl> vNodes = new ArrayList<>();

    void init(List<Machine> allMachineList, int idxInAllMachineList) {
        this.allMachineList = allMachineList;
        this.idxInAllMachineList = idxInAllMachineList;
    }

    void setMachineId(String machineId) {
        this.machineId = machineId;
    }

    void finishSetup() {
        allMachineList = ImmutableList.copyOf(allMachineList); // should already be immutable
        vNodes = ImmutableList.copyOf(vNodes);
    }

    void setState(MachineInRingState state) {
        this.state = state;
    }

    public String getMachineId() {
        return machineId;
    }

    public static List<MachineInfo> asMachineInfo(@Nullable Collection<Machine> input) {
        List<MachineInfo> result = new ArrayList<>();
        if (input != null) {
            for (Machine m : input) {
                result.add(m.asMachineInfo());
            }
        }
        return result;
    }

    public MachineInfo asMachineInfo() {
        return new MachineInfo() {
            @Override
            public String getId() {
                return getMachineId();
            }

            @Override
            public int getVnodeCount() {
                return getVNodes() != null ? getVNodes().size() : 0;
            }

            @Override
            public MachineInRingState getState() {
                return Machine.this.getState();
            }

            @Override
            public String toString() {
                return "MachineMachineInfo{" +
                        "id='" + getId() + '\'' +
                        ", state=" + getState() +
                        '}';
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (!(o instanceof MachineInfo)) return false;

                MachineInfo info = (MachineInfo) o;

                if (getId() != null ? !getId().equals(info.getId()) : info.getId() != null) return false;

                return true;
            }

            @Override
            public int hashCode() {
                return getId() != null ? getId().hashCode() : 0;
            }
        };
    }

    public MachineInRingState getState() {
        return state;
    }

    /**
     * Depending on ring this might be a sum of led vnodes and replica vnodes or only led vnodes.
     */
    public List<VNodeImpl> getVNodes() {
        return vNodes;
    }

    public Iterable<Machine> getReplicaIterable() {
        return new Iterable<Machine>() {
            @Override
            public Iterator<Machine> iterator() {
                return new Iterator<Machine>() {

                    int nextPos = calcNextIdx(idxInAllMachineList);

                    private int calcNextIdx(int current) {
                        return  (current + 1) % allMachineList.size();
                    }

                    @Override
                    public boolean hasNext() {
                        return nextPos != idxInAllMachineList;
                    }

                    @Override
                    public Machine next() {
                        if (!hasNext()) {
                            throw new IllegalStateException("Iterator exhausted");
                        }
                        Machine result = allMachineList.get(nextPos);
                        nextPos = calcNextIdx(nextPos);
                        return result;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Machine)) return false;

        Machine machine = (Machine) o;

        if (machineId != null ? !machineId.equals(machine.machineId) : machine.machineId != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return machineId != null ? machineId.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "Machine{" +
                "machineId='" + machineId + '\'' +
                ", state=" + state +
                ", vnodeCount=" + getVNodes().size() +
                '}';
    }
}
