package org.mpisws.hitmc.server.executor;

import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.event.LeaderToFollowerMessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LeaderToFollowerMessageExecutor extends BaseEventExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(MessageExecutor.class);

    private final TestingService testingService;

    public LeaderToFollowerMessageExecutor(final TestingService testingService) {
        this.testingService = testingService;
    }

    @Override
    public boolean execute(final LeaderToFollowerMessageEvent event) throws IOException {
        if (event.isExecuted()) {
            LOG.info("Skipping an executed learner handler message event: {}", event.toString());
            return false;
        }
        LOG.debug("Releasing message: {}", event.toString());
        testingService.releaseMessageToFollower(event);
        testingService.waitAllNodesSteady();
        // TODO: wait for later event
        event.setExecuted();
        LOG.debug("Learner handler message executed: {}\n\n\n", event.toString());
        return true;
    }
}
