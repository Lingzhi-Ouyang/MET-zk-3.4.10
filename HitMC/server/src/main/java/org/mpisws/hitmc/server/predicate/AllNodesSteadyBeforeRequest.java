package org.mpisws.hitmc.server.predicate;

import org.mpisws.hitmc.api.NodeState;
import org.mpisws.hitmc.api.SubnodeType;
import org.mpisws.hitmc.api.state.LeaderElectionState;
import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Pre-condition for client requests
 * Use NodePhases?
 */
public class AllNodesSteadyBeforeRequest implements WaitPredicate{
    private static final Logger LOG = LoggerFactory.getLogger(AllNodesSteadyBeforeRequest.class);

    private final TestingService testingService;

    public AllNodesSteadyBeforeRequest(final TestingService testingService) {
        this.testingService = testingService;
    }

    @Override
    public boolean isTrue() {
        // TODO: is it needed to combine AllNodesSteady?
        for (int nodeId = 0; nodeId < testingService.getSchedulerConfiguration().getNumNodes(); ++nodeId) {
            final NodeState nodeState = testingService.getNodeStates().get(nodeId);
            switch (nodeState) {
                case STARTING:
                case STOPPING:
                    LOG.debug("------Not steady-----Node {} status: {}\n", nodeId, nodeState);
                    return false;
                case OFFLINE:
                    LOG.debug("-----------Node {} status: {}", nodeId, nodeState);
                    continue;
                case ONLINE:
                    LOG.debug("-----------Node {} status: {}", nodeId, nodeState);
            }
            LeaderElectionState leaderElectionState = testingService.getLeaderElectionStates().get(nodeId);
            // TODO: LOOKING ???
            if (LeaderElectionState.LEADING.equals(leaderElectionState)) {
                if (!leaderSteadyBeforeRequest(nodeId)) {
                    return false;
                }
            } else if (LeaderElectionState.FOLLOWING.equals(leaderElectionState)) {
                if (!followerSteadyBeforeRequest(nodeId)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public String describe() {
        return "all nodes steady before request";
    }

    private boolean leaderSteadyBeforeRequest(final int nodeId) {
        boolean syncExisted = false;
        // Note: learnerHandlerSender is created by learnerHandler so here we do not make a flag for learnerHandler
        boolean learnerHandlerSenderExisted = false;
        for (final Subnode subnode : testingService.getSubnodeSets().get(nodeId)) {
            if (SubnodeType.SYNC_PROCESSOR.equals(subnode.getSubnodeType())) {
                syncExisted = true;
            } else if (SubnodeType.LEARNER_HANDLER_SENDER.equals(subnode.getSubnodeType())) {
                learnerHandlerSenderExisted = true;
            }
            LOG.debug("-----------Leader node {} subnode {} status: {}, subnode type: {}",
                    nodeId, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
        }
        return syncExisted && learnerHandlerSenderExisted;
    }

    private boolean followerSteadyBeforeRequest(final int nodeId) {
        boolean syncExisted = false;
        boolean followerProcessorExisted = false;
        for (final Subnode subnode : testingService.getSubnodeSets().get(nodeId)) {
            if (SubnodeType.SYNC_PROCESSOR.equals(subnode.getSubnodeType())) {
                syncExisted = true;
            } else if (SubnodeType.FOLLOWER_PROCESSOR.equals(subnode.getSubnodeType())) {
                followerProcessorExisted = true;
            }
            LOG.debug("-----------Follower node {} subnode {} status: {}, subnode type: {}",
                    nodeId, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
        }
        return syncExisted && followerProcessorExisted;
    }
}
