package org.mpisws.hitmc.server.predicate;

import org.mpisws.hitmc.api.NodeState;
import org.mpisws.hitmc.server.TestingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Wait Predicate for the end of an execution.
 */
public class AllNodesVoted implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(AllNodesVoted.class);

    private final TestingService testingService;

    public AllNodesVoted(final TestingService testingService) {
        this.testingService = testingService;
    }

    @Override
    public boolean isTrue() {
        for (int nodeId = 0; nodeId < testingService.getSchedulerConfiguration().getNumNodes(); ++nodeId) {
            LOG.debug("nodeId: {}, state: {}, votes: {}", nodeId, testingService.getNodeStates().get(nodeId), testingService.getVotes().get(nodeId));
            if (!NodeState.OFFLINE.equals(testingService.getNodeStates().get(nodeId))
                    && (!NodeState.ONLINE.equals(testingService.getNodeStates().get(nodeId)) || testingService.getVotes().get(nodeId) == null)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String describe() {
        return "allNodesVoted";
    }
}
