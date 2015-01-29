package de.deverado.framework.concurrent;

public interface StartupListenerWithRunlevel extends StartupListener, RunlevelProvider {

    String getRunlevel();
}
