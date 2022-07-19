package org.mpisws.hitmc.server.predicate;

import org.mpisws.hitmc.api.NodeState;
import org.mpisws.hitmc.api.SubnodeState;
import org.mpisws.hitmc.api.configuration.SchedulerConfiguration;
import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/***
 * Wait Predicate for the first message of each node after election
 *
 */
public class NewMessageOffered implements WaitPredicate {
    private static final Logger LOG = LoggerFactory.getLogger(NewMessageOffered.class);

    private final TestingService testingService;

    public NewMessageOffered(final TestingService testingService) {
        this.testingService = testingService;
    }

    @Override
    public boolean isTrue() {
        // if there exists one node offering a new message
        for (int nodeId = 0; nodeId < testingService.getSchedulerConfiguration().getNumNodes(); ++nodeId) {
            final NodeState nodeState = testingService.getNodeStates().get(nodeId);
            if (NodeState.ONLINE.equals(nodeState)) {
                for (final Subnode subnode : testingService.getSubnodeSets().get(nodeId)) {
                    if (SubnodeState.SENDING.equals(subnode.getState())) {
                        LOG.debug("------NewMessageOffered-----Node {} status: {}, subnode {} status: {}, is main receiver : {}",
                                nodeId, nodeState, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
                        return true;
                    }
                    LOG.debug("-----------Node {} status: {}, subnode {} status: {}, is main receiver : {}",
                            nodeId, nodeState, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
                }
            }
        }
        LOG.debug("------New message has not yet come-----");
        return false;
    }

    @Override
    public String describe() {
        return "newMessageOffered";
    }
}
