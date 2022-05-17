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
            LOG.info("Skipping an executed request processor event: {}", event.toString());
            return false;
        }
        LOG.debug("Processing request: {}", event.toString());
        testingService.releaseRequestProcessor(event);
        switch (event.getSubnodeType()) {
            case SYNC_PROCESSOR:
                if (quorumSynced(event.getZxid())){
                    // If learnerHandler's COMMIT is not intercepted
//                    testingService.waitQuorumToCommit(event);
                    testingService.waitAllNodesSteadyAfterQuorumSynced();
                } else {
                    testingService.waitAllNodesSteady();
                }
                break;
            case COMMIT_PROCESSOR:
                testingService.waitCommitProcessorDone(event.getId(), event.getNodeId());
                testingService.waitAllNodesSteady();
                break;
        }
        event.setExecuted();
        LOG.debug("Request processor event executed: {}\n\n\n", event.toString());
        return true;
    }

    public boolean quorumSynced(final long zxid) {
        if (testingService.getZxidSyncedMap().containsKey(zxid)){
            final int count = testingService.getZxidSyncedMap().get(zxid);
            final int nodeNum = testingService.getSchedulerConfiguration().getNumNodes();
            return count > nodeNum / 2;
        }
        return false;
    }
}
