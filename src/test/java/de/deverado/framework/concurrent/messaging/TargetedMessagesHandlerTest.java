package de.deverado.framework.concurrent.messaging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import de.deverado.framework.messaging.api.Message;
import de.deverado.framework.messaging.api.MessageHandler;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@ParametersAreNonnullByDefault
public class TargetedMessagesHandlerTest {

    private static final Logger LOG = LoggerFactory.getLogger(TargetedMessagesHandlerTest.class);

    @Test
    public void testHandle() throws Exception {
        TargetedMessagesDistributingHandler cut = new TargetedMessagesDistributingHandler();
        TargetedTestHandler somekeyHandler = new TargetedTestHandler("somekey");
        TargetedTestHandler defaultHandler = new TargetedTestHandler("default");
        cut.init(Arrays.asList(new TargetedHandler[]{
                defaultHandler,
                somekeyHandler,
        }));

        Message unknownKeyMsg = createMockMessage("blah");
        assertNotNull(cut.handle(unknownKeyMsg));
        Mockito.verify(unknownKeyMsg, Mockito.never()).reject(true, null);
        Mockito.verify(unknownKeyMsg, Mockito.never()).acknowledge();

        // unknown - dropping
        assertEquals(0, defaultHandler.getHandledMessages().size());
        assertEquals(0, somekeyHandler.getHandledMessages().size());

        assertNotNull(cut.handle(createMockMessage("somekey")));
        assertEquals(0, defaultHandler.getHandledMessages().size());
        assertEquals(1, somekeyHandler.getHandledMessages().size());

        assertNotNull(cut.handle(createMockMessage(null)));
        assertEquals(1, defaultHandler.getHandledMessages().size());
        assertEquals(1, somekeyHandler.getHandledMessages().size());

    }

    @Test
    public void testHandleWithUnknownFunctionIdHandler() throws Exception {
        TargetedMessagesDistributingHandler cut = new TargetedMessagesDistributingHandler();
        TargetedTestHandler unknownHandler = new TargetedTestHandler(TargetedMessagesDistributingHandler.UNKNOWN_FUNCTION_ID);
        TargetedTestHandler defaultHandler = new TargetedTestHandler("default");
        cut.init(Arrays.asList(new TargetedHandler[]{
                defaultHandler,
                unknownHandler,
        }));

        Message unknownKeyMsg = createMockMessage("blah");
        assertNotNull(cut.handle(unknownKeyMsg));
        Mockito.verify(unknownKeyMsg).acknowledge();

        assertEquals(0, defaultHandler.getHandledMessages().size());
        assertEquals(1, unknownHandler.getHandledMessages().size());

        assertNotNull(cut.handle(createMockMessage(null)));
        Mockito.verify(unknownKeyMsg).acknowledge();
        assertEquals(1, defaultHandler.getHandledMessages().size());
        assertEquals(1, unknownHandler.getHandledMessages().size());

    }

    @Test
    public void testHandleWithRetryingNoHandler() throws Exception {
        TargetedMessagesDistributingHandler cut = new TargetedMessagesDistributingHandler();
        cut.setNoHandlerPolicy(new TargetedMessagesDistributingHandler.RetryingNoHandlerPolicy(LOG, "", null));

        TargetedTestHandler defaultHandler = new TargetedTestHandler("default");
        cut.init(Arrays.asList(new TargetedHandler[]{
                defaultHandler,
        }));

        Message unknownKeyMsg = createMockMessage("blah");
        assertNotNull(cut.handle(unknownKeyMsg));
        Mockito.verify(unknownKeyMsg).reject(true, null);
        Mockito.verify(unknownKeyMsg, Mockito.never()).acknowledge();

        assertEquals(0, defaultHandler.getHandledMessages().size());

        assertNotNull(cut.handle(createMockMessage(null)));
        assertEquals(1, defaultHandler.getHandledMessages().size());

    }

    private Message createMockMessage(String target) {
        Message mock = Mockito.mock(Message.class);
        Mockito.when(mock.getProperty(TargetedMessagesDistributingHandler.TARGET_FK_PROP)).thenReturn(target);
        Mockito.when(mock.acknowledge()).thenAnswer(new Answer<ListenableFuture<?>>() {
            @Override
            public ListenableFuture<?> answer(InvocationOnMock invocation) throws Throwable {
                return Futures.immediateFuture(Boolean.TRUE);
            }
        });

        Mockito.when(mock.reject(true, null)).thenAnswer(new Answer<ListenableFuture<?>>() {
            @Override
            public ListenableFuture<?> answer(InvocationOnMock invocation) throws Throwable {
                return Futures.immediateFuture(Boolean.TRUE);
            }
        });
        return mock;
    }

    @ParametersAreNonnullByDefault
    public static class TargetedTestHandler implements TargetedHandler {

        private MessageHandler handler;
        private String target;
        private List<Message> handledMessages = new ArrayList<>();

        public TargetedTestHandler(String target) {
            this.target = target;
            handler = new MessageHandler() {
                @Nullable
                @Override
                public ListenableFuture<?> handle(Message msg) {
                    handledMessages.add(msg);
                    return msg.acknowledge();
                }
            };
        }

        @Override
        public String getTarget() {
            return target;
        }

        @Override
        public MessageHandler getHandler() {
            return handler;
        }

        public List<Message> getHandledMessages() {
            return handledMessages;
        }
    }
}