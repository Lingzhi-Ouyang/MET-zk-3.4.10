package org.mpisws.hitmc.server.executor;

import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.event.InternalEvent;
import org.mpisws.hitmc.server.event.LearnerHandlerMessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class InternalEventExecutor extends BaseEventExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(MessageExecutor.class);

    private final TestingService testingService;

    public InternalEventExecutor(final TestingService testingService) {
        this.testingService = testingService;
    }

    @Override
    public boolean execute(final InternalEvent event) throws IOException {
        if (event.isExecuted()) {
            LOG.info("Skipping an executed internal message event: {}", event.toString());
            return false;
        }
        LOG.debug("Releasing message: {}", event.toString());
        testingService.releaseInternalMessage(event);
        testingService.waitAllNodesSteady();
        event.setExecuted();
        LOG.debug("Internal message event executed: {}\n\n\n", event.toString());
        return true;
    }
}
