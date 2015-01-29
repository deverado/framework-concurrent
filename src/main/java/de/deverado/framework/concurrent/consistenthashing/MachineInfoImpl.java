package de.deverado.framework.concurrent.consistenthashing;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

public class MachineInfoImpl {

    private String id;

    private int vnodeCount;

    private MachineInRingState state;

    public static MachineInfoImpl create(String id, int vnodeCount, MachineInRingState state) {
        MachineInfoImpl result = new MachineInfoImpl();
        result.id = id;
        result.vnodeCount = vnodeCount;
        result.state = state;
        return result;
    }

    public String getId() {
        return id;
    }

    public int getVnodeCount() {
        return vnodeCount;
    }

    public MachineInRingState getState() {
        return state;
    }

    @Override
    public String toString() {
        return "MachineInfo{" +
                "id='" + id + '\'' +
                ", state=" + state +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MachineInfoImpl)) return false;

        MachineInfoImpl that = (MachineInfoImpl) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }
}
