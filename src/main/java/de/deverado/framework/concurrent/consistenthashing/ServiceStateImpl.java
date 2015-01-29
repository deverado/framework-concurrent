package de.deverado.framework.concurrent.consistenthashing;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

public class ServiceStateImpl implements ServiceState {
    private String serviceName;
    private MachineInRingState state;

    public static ServiceStateImpl create(String serviceName, MachineInRingState state) {
        ServiceStateImpl result = new ServiceStateImpl();
        result.serviceName = serviceName;
        result.state = state;
        return result;
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    @Override
    public MachineInRingState getState() {
        return state;
    }
}
