package de.deverado.framework.concurrent;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Used to be notified about (guice) shutdown.
 *
 * @see ShutdownListenerRunner
 * @see RunlevelShutdownListenerRunner
 */
public interface ShutdownListener {

    ListenableFuture<?> shutdown();
}
