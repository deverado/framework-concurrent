package de.deverado.framework.concurrent;/*
 * Copyright Georg Koester 2012-15. All rights reserved.
 */

public interface RunlevelProvider {

    /**
     * Runlevels go from low to high (string ordering) and services in the the runlevels are started in that order
     * and shut down in reverse string order. So services in runlevel 0 are all started before services in runlevel 1
     * are begun to be started.
     */
    String getRunlevel();
}
