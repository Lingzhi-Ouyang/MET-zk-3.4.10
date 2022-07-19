package org.mpisws.hitmc.server.predicate;

import org.mpisws.hitmc.api.NodeState;
import org.mpisws.hitmc.api.SubnodeState;
import org.mpisws.hitmc.api.SubnodeType;
import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubnodeInSendingState implements WaitPredicate{
    private static final Logger LOG = LoggerFactory.getLogger(SubnodeInSendingState.class);

    private final TestingService testingService;

    private final int subnodeId;

    public SubnodeInSendingState(final TestingService testingService,
                                     final int subnodeId) {
        this.testingService = testingService;
        this.subnodeId = subnodeId;
    }

    @Override
    public boolean isTrue() {
        final Subnode subnode = testingService.getSubnodes().get(subnodeId);
        final int nodeId = subnode.getNodeId();
        final NodeState nodeState = testingService.getNodeStates().get(nodeId);
        if (NodeState.ONLINE.equals(nodeState)) {
            return SubnodeState.SENDING.equals(subnode.getState());
        }
        return true;
    }

    @Override
    public String describe() {
        return " Subnode " + subnodeId + " is in sending state";
    }

}
