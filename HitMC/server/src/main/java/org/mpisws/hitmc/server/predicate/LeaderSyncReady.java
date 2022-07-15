package org.mpisws.hitmc.server.predicate;

import org.mpisws.hitmc.api.NodeState;
import org.mpisws.hitmc.api.SubnodeState;
import org.mpisws.hitmc.api.SubnodeType;
import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class LeaderSyncReady implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderSyncReady.class);

    private final TestingService testingService;

    private final int leaderId;
    private final List<Integer> peers;

    public LeaderSyncReady(final TestingService testingService,
                           final int leaderId,
                           final List<Integer> peers) {
        this.testingService = testingService;
        this.leaderId = leaderId;
        this.peers = peers;
    }

    @Override
    public boolean isTrue() {
//        return testingService.getLeaderSyncFollowerCountMap().get(leaderId) == 0;

        // method 1: check if the follower's corresponding learner handler sender is in SENDING state
        // method 2:
        List<Integer> followerLearnerHandlerSenderMap = testingService.getFollowerLearnerHandlerSenderMap();
        if (peers != null) {
            for (Integer peer: peers) {
                final Integer subnodeId = followerLearnerHandlerSenderMap.get(peer);
                if (subnodeId == null) return false;
                Subnode subnode = testingService.getSubnodes().get(subnodeId);
                assert subnode.getSubnodeType().equals(SubnodeType.LEARNER_HANDLER_SENDER);
                if (!subnode.getState().equals(SubnodeState.SENDING)){
//                    LOG.debug();
                    return false;
                }
            }
        }
        else {
            for (int nodeId = 0; nodeId < testingService.getSchedulerConfiguration().getNumNodes(); ++nodeId) {
                final Integer subnodeId = followerLearnerHandlerSenderMap.get(nodeId);
                if (subnodeId == null) return false;
                Subnode subnode = testingService.getSubnodes().get(subnodeId);
                assert subnode.getSubnodeType().equals(SubnodeType.LEARNER_HANDLER_SENDER);
                if (!subnode.getState().equals(SubnodeState.SENDING)){
                    return  false;
                }
            }
        }
        return true;
    }

    @Override
    public String describe() {
        return " Leader " + leaderId + " sync ready with peers: " + peers;
    }

}
