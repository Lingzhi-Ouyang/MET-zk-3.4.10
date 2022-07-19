package org.mpisws.hitmc.server.predicate;

import org.mpisws.hitmc.api.NodeState;
import org.mpisws.hitmc.api.SubnodeState;
import org.mpisws.hitmc.api.state.LeaderElectionState;
import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class AliveNodesInLookingState implements WaitPredicate{

    private static final Logger LOG = LoggerFactory.getLogger(AliveNodesInLookingState.class);

    private final TestingService testingService;

    private final List<Integer> peers;

    public AliveNodesInLookingState(final TestingService testingService) {
        this.testingService = testingService;
        peers = null;
    }

    public AliveNodesInLookingState(final TestingService testingService, final List<Integer> peers) {
        this.testingService = testingService;
        this.peers = peers;
    }



    @Override
    public boolean isTrue() {
        if (peers != null) {
            for (Integer nodeId : peers) {
                if (checkNodeNotLooking(nodeId)) return false;
            }
        } else {
            for (int nodeId = 0; nodeId < testingService.getSchedulerConfiguration().getNumNodes(); ++nodeId) {
                if (checkNodeNotLooking(nodeId)) return false;
            }
        }
        return true;
    }

    private boolean checkNodeNotLooking(Integer nodeId) {
        final NodeState nodeState = testingService.getNodeStates().get(nodeId);
        LeaderElectionState leaderElectionState = testingService.getLeaderElectionStates().get(nodeId);
        switch (nodeState) {
            case STARTING:
            case STOPPING:
                LOG.debug("------Not steady-----Node {} status: {}\n", nodeId, nodeState);
                return true;
            case ONLINE:
                if (!LeaderElectionState.LOOKING.equals(leaderElectionState)) {
                    LOG.debug("------Not steady-----Node {} leaderElectionState: {}\n",
                            nodeId, leaderElectionState);
                    return true;
                }
                LOG.debug("-----------Node {} status: {}", nodeId, nodeState);
                break;
            case OFFLINE:
                LOG.debug("-----------Node {} status: {}", nodeId, nodeState);
        }
        for (final Subnode subnode: testingService.getSubnodeSets().get(nodeId)) {
            if (SubnodeState.PROCESSING.equals(subnode.getState())) {
                LOG.debug("------Not steady-----Node {} subnode {} status: {}, subnode type: {}\n",
                        nodeId, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
                return true;
            }
            else {
                LOG.debug("-----------Node {} subnode {} status: {}, subnode type: {}",
                        nodeId, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
            }
        }
        return false;
    }

    @Override
    public String describe() {
        if (peers == null) return "all nodes in LOOKING state";
        else return peers + " in LOOKING state";
    }
}
