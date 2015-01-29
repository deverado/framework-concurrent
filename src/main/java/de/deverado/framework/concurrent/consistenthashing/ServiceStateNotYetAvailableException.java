package de.deverado.framework.concurrent.consistenthashing;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

/**
 * Use this to show that a service should be required later for the state - because
 * at the moment it cannot deliver a state. ATTENTION: The service MUST NOT postpone
 * forever - this disables state management on the node.
 */
public class ServiceStateNotYetAvailableException extends Exception {

    public ServiceStateNotYetAvailableException() {
    }

    public ServiceStateNotYetAvailableException(String message) {
        super(message);
    }

    public ServiceStateNotYetAvailableException(String message, Throwable cause) {
        super(message, cause);
    }

    public ServiceStateNotYetAvailableException(Throwable cause) {

        super(cause);
    }

    public ServiceStateNotYetAvailableException(String message, Throwable cause, boolean enableSuppression,
                                                boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
