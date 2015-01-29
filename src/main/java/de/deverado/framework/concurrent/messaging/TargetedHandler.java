package de.deverado.framework.concurrent.messaging;


import de.deverado.framework.messaging.api.MessageHandler;

public interface TargetedHandler {

    String getTarget();

    MessageHandler getHandler();

}
