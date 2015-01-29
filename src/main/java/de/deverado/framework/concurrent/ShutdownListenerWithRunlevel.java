package de.deverado.framework.concurrent;

/**
 * Used to be notified about (guice) shutdown.
 *
 * @see ShutdownListenerRunner
 * @see RunlevelShutdownListenerRunner
 */
public interface ShutdownListenerWithRunlevel extends ShutdownListener, RunlevelProvider {

    String getRunlevel();
}
