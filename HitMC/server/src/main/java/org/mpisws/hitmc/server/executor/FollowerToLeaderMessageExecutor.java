package org.mpisws.hitmc.server.executor;

import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.event.FollowerToLeaderMessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FollowerToLeaderMessageExecutor extends BaseEventExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(MessageExecutor.class);

    private final TestingService testingService;

    public FollowerToLeaderMessageExecutor(final TestingService testingService) {
        this.testingService = testingService;
    }

    @Override
    public boolean execute(final FollowerToLeaderMessageEvent event) throws IOException {
        if (event.isExecuted()) {
            LOG.info("Skipping an executed follower message event: {}", event.toString());
            return false;
        }
        LOG.debug("Releasing message: {}", event.toString());
        testingService.releaseInternalMessage(event);
        testingService.waitAllNodesSteady();
        event.setExecuted();
        LOG.debug("Follower message event executed: {}\n\n\n", event.toString());
        return true;
    }
}
