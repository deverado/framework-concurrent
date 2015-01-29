package de.deverado.framework.concurrent.messaging;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import de.deverado.framework.core.problemreporting.LoggingProblemReporter;
import de.deverado.framework.core.problemreporting.ProblemReporter;
import de.deverado.framework.messaging.api.Message;
import de.deverado.framework.messaging.api.MessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Handles messages, distributing them to handlers given during initialization.
 * Can deal with untargeted and targeted messages, can distribute targeted messages
 * for which no handler is known to a fallback handler {@see TargetedMessagesDistributingHandler#UNKNOWN_FUNCTION_ID}.
 *
 * After initialization threadsafe.
 *
 * Was in framework.messaging before and called MultiMessageTypeHandler.
 */
@ParametersAreNonnullByDefault
public class TargetedMessagesDistributingHandler implements MessageHandler {

    public static final String TARGET_FK_PROP = "fk";

    /**
     * Use this to set a handler for unknown function ids.
     */
    public static final String UNKNOWN_FUNCTION_ID = "unknownFunctionId";

    /**
     * Use this to set a handler for a msg with null function id.
     */
    public static final String DEFAULT_FUNCTION_ID = "default";

    private static final Logger LOG = LoggerFactory.getLogger(TargetedMessagesDistributingHandler.class);

    private ImmutableMap<String, MessageHandler> handlers;

    private NoHandlerPolicy noHandlerPolicy = THROWING_NO_HANDLER_POLICY;

    ProblemReporter problemReporter = new LoggingProblemReporter();

    public TargetedMessagesDistributingHandler() {
    }

    /**
     * If the argument is <code>null</code> a {@link LoggingProblemReporter} is
     * installed.
     */
    public TargetedMessagesDistributingHandler setProblemReporter(@Nullable ProblemReporter problemReporter) {
        if (problemReporter == null) {
            problemReporter = new LoggingProblemReporter();
        }
        this.problemReporter = problemReporter;
        return this;
    }

    public ProblemReporter getProblemReporter() {
        return problemReporter;
    }

    public TargetedMessagesDistributingHandler setNoHandlerPolicy(
            NoHandlerPolicy noHandlerPolicy) {
        Preconditions.checkArgument(noHandlerPolicy != null);
        this.noHandlerPolicy = noHandlerPolicy;
        return this;
    }

    /**
     * Sets up a message handler that routes to targeted handler refs and does
     * no acknowledging. If no targeted handler the noHandlerPolicy steps in.
     *
     */
    public TargetedMessagesDistributingHandler init(
            @Nullable Collection<TargetedHandler> handlerColl) {

        prepareHandlers(handlerColl == null ?
                Collections.<TargetedHandler> emptyList() : handlerColl);
        return this;
    }

    @Override
    public ListenableFuture<?> handle(Message message) {
        String functionId = null;
        MessageHandler messageHandler = null;
        try {
            functionId = message.getProperty(TARGET_FK_PROP);
            if (functionId == null) {
                functionId = DEFAULT_FUNCTION_ID;
            }
            messageHandler = handlers.get(functionId);
            if (messageHandler == null) {
                messageHandler = handlers.get(UNKNOWN_FUNCTION_ID);
            }
            if (messageHandler == null) {
                return noHandlerPolicy.handle(functionId, message, handlers);
            } else {
                return messageHandler.handle(message);
            }
        } catch (Exception e) {
            problemReporter.warn(LOG,
                    "Problem with message handler for {}: {}: " + e,
                    functionId, messageHandler);
            LOG.debug("Problem with message handler for {}", functionId, e);
            return Futures.immediateFailedFuture(e);
        }
    }

    private void prepareHandlers(Collection<TargetedHandler> handlerColl) {

        Map<String, MessageHandler> tempMap = Maps.newHashMap();
        for (TargetedHandler p : handlerColl) {
            MessageHandler previous = tempMap.put(p.getTarget(), p.getHandler());
            if (previous != null) {
                throw new IllegalArgumentException("Collision of handlers for "
                        + p.getTarget() + ": " + ImmutableList.of(previous, p));
            }
        }
        synchronized (this) {
            Preconditions.checkState(this.handlers == null,
                    "already initialized");

            this.handlers = ImmutableMap.copyOf(tempMap);
        }
    }

    public static final NoHandlerPolicy THROWING_NO_HANDLER_POLICY = new ThrowingNoHandlerPolicy();

    public interface NoHandlerPolicy {

        ListenableFuture<?> handle(String functionId, Message message,
                    ImmutableMap<String, MessageHandler> handlers);

    }

    public static class ThrowingNoHandlerPolicy implements NoHandlerPolicy {

        @Override
        public ListenableFuture<?> handle(String functionId, Message message,
                                          ImmutableMap<String, MessageHandler> handlers) {
            throw new RuntimeException("No handler for " + functionId
                    + " and message: " + message);
        }
    }

    public static class DebugLoggingNoHandlerPolicy implements NoHandlerPolicy {

        private final Logger log;
        private final String logPrefix;

        public DebugLoggingNoHandlerPolicy(Logger log, String logPrefix) {
            this.log = log;
            this.logPrefix = logPrefix;
        }


        @Override
        public ListenableFuture<?> handle(String functionId, Message message,
                                          ImmutableMap<String, MessageHandler> handlers) {
            log.debug("" + logPrefix + ": No handler (message ignored) for " + message);
            return Futures.immediateFuture(Boolean.FALSE);
        }
    }

    /**
     * Rejects the message with retry possibility {@link Message#reject(boolean, Long)}. Logs
     * to debug. Retry might get the message to a different host (Depends on sharding. Mind that message might get new
     * id on retry depending on implementation...).
     */
    public static class RetryingNoHandlerPolicy implements NoHandlerPolicy {

        private final Logger log;
        private final String logPrefix;
        private final Long optionalRetryDelay;

        public RetryingNoHandlerPolicy(Logger log, String logPrefix, @Nullable Long optionalRetryDelay) {
            this.log = log;
            this.logPrefix = logPrefix;
            this.optionalRetryDelay = optionalRetryDelay;
        }


        @Override
        public ListenableFuture<?> handle(String functionId, Message message,
                                          ImmutableMap<String, MessageHandler> handlers) {
            log.debug("" + logPrefix + ": No handler (message retried) for " + message);
            Long retryTime = null;
            if (optionalRetryDelay != null && optionalRetryDelay >= 0) {
                retryTime = System.currentTimeMillis() + optionalRetryDelay;
            }
            return message.reject(true, retryTime);
        }
    }

}
