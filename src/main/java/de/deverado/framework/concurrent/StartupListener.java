package de.deverado.framework.concurrent;

import com.google.common.util.concurrent.ListenableFuture;

public interface StartupListener {

    ListenableFuture<?> startup() throws Exception;
}
