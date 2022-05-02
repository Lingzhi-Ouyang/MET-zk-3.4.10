package org.mpisws.hitmc.server.predicate;

import org.mpisws.hitmc.api.NodeState;
import org.mpisws.hitmc.api.NodeStateForClientRequest;
import org.mpisws.hitmc.api.configuration.SchedulerConfiguration;
import org.mpisws.hitmc.server.TestingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/***
 * Wait Predicate for the end of a client mutation event
 */
public class AllNodesSteadyForClientMutation implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(AllNodesSteadyForClientMutation.class);

    private final TestingService testingService;

    public AllNodesSteadyForClientMutation(final TestingService testingService) {
        this.testingService = testingService;
    }

    @Override
    public boolean isTrue() {
        for (int nodeId = 0; nodeId < testingService.getSchedulerConfiguration().getNumNodes(); ++nodeId) {
            final NodeState nodeState = testingService.getNodeStates().get(nodeId);
            if (NodeState.STARTING.equals(nodeState) || NodeState.STOPPING.equals(nodeState) ) {
                LOG.debug("------not steady-----Node {} status: {}",
                        nodeId, nodeState);
                return false;
            }
            final NodeStateForClientRequest nodeStateForClientRequest
                    = testingService.getNodeStateForClientRequests().get(nodeId);
            if ( NodeStateForClientRequest.SET_PROCESSING.equals(nodeStateForClientRequest)){
                LOG.debug("------not steady-----Node {} nodeStateForClientRequest: {}",
                        nodeId, nodeStateForClientRequest);
                return false;
            }
        }
        return true;
    }

    @Override
    public String describe() {
        return "AllNodesSteadyForClientMutation";
    }
}
