package de.deverado.framework.concurrent.consistenthashing;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

public interface ServiceStateProvider {

    /**
     * Implement so as to be fast
     * @return the current state of a service
     * @throws de.deverado.framework.concurrent.consistenthashing.ServiceStateNotYetAvailableException
     * Throw this to postpone the state query - e.g. during init or at an inopportune moment. The
     * state will be queried later.
     */
    public ServiceState getCurrentState() throws ServiceStateNotYetAvailableException;

}
