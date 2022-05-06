package org.mpisws.hitmc.server.executor;

import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.event.RequestEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RequestProcessorExecutor extends BaseEventExecutor{
    private static final Logger LOG = LoggerFactory.getLogger(RequestProcessorExecutor.class);

    private final TestingService testingService;

    public RequestProcessorExecutor(final TestingService testingService) {
        this.testingService = testingService;
    }

    @Override
    public boolean execute(final RequestEvent event) throws IOException {
        if (event.isExecuted()) {
            LOG.info("Skipping an executed log event: {}", event.toString());
            return false;
        }
        LOG.debug("Logging request: {}", event.toString());
        testingService.releaseRequestProcessor(event);
        testingService.waitAllNodesSteady();
        event.setExecuted();
        LOG.debug("LogRequest executed: {}", event.toString());
        return true;
    }
}
