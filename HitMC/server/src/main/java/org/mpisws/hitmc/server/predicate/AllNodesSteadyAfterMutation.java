package org.mpisws.hitmc.server.predicate;

import org.mpisws.hitmc.api.NodeState;
import org.mpisws.hitmc.api.SubnodeState;
import org.mpisws.hitmc.api.SubnodeType;
import org.mpisws.hitmc.api.state.LeaderElectionState;
import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Wait Predicate for client request event when
 * both learnerHandlerSender and syncProcessor are intercepted
 * - leader: SYNC_PROCESSOR in SENDING state && LEARNER_HANDLER in SENDING states, other subnodes in SENDING / RECEIVING states
 * - follower: SYNC_PROCESSOR && FOLLOWER_PROCESSOR in SENDING / RECEIVING states
 */
public class AllNodesSteadyAfterMutation implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(AllNodesSteadyAfterMutation.class);

    private final TestingService testingService;

    public AllNodesSteadyAfterMutation(final TestingService testingService) {
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
                    LOG.debug("------Not steady-----Node {} status: {}", nodeId, nodeState);
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
                if (!leaderSteadyAfterMutation(nodeId)) {
                    return false;
                }
            } else if (LeaderElectionState.FOLLOWING.equals(leaderElectionState)) {
                if (!followerSteadyAfterMutation(nodeId)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public String describe() {
        return "all nodes steady after mutation";
    }

    private boolean leaderSteadyAfterMutation(final int nodeId) {
        boolean syncExisted = false;
        // Note: learnerHandlerSender is created by learnerHandler so here we do not make a flag for learnerHandler
        boolean learnerHandlerSenderExisted = false;
        for (final Subnode subnode : testingService.getSubnodeSets().get(nodeId)) {
            if (SubnodeType.SYNC_PROCESSOR.equals(subnode.getSubnodeType())) {
                syncExisted = true;
                if (!SubnodeState.SENDING.equals(subnode.getState())) {
                    LOG.debug("------Not steady for leader's {} thread-----" +
                                    "Node {} subnode {} status: {}",
                            subnode.getSubnodeType(), nodeId, subnode.getId(), subnode.getState());
                    return false;
                }
            } else if (SubnodeType.LEARNER_HANDLER_SENDER.equals(subnode.getSubnodeType())) {
                learnerHandlerSenderExisted = true;
                if (!SubnodeState.SENDING.equals(subnode.getState())) {
                    LOG.debug("------Not steady for leader's {} thread-----" +
                                    "Node {} subnode {} status: {}",
                            subnode.getSubnodeType(), nodeId, subnode.getId(), subnode.getState());
                    return false;
                }
            } else if (SubnodeState.PROCESSING.equals(subnode.getState())) {
                LOG.debug("------Not steady for leader's {} thread-----" +
                                "Node {} subnode {} status: {}",
                        subnode.getSubnodeType(), nodeId, subnode.getId(), subnode.getState());
                return false;
            }
            LOG.debug("-----------Leader node {} subnode {} status: {}, subnode type: {}",
                        nodeId, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
        }
        return syncExisted && learnerHandlerSenderExisted;
    }

    private boolean followerSteadyAfterMutation(final int nodeId) {
        boolean syncExisted = false;
        boolean followerProcessorExisted = false;
        for (final Subnode subnode : testingService.getSubnodeSets().get(nodeId)) {
            if (SubnodeType.SYNC_PROCESSOR.equals(subnode.getSubnodeType())) {
                syncExisted = true;
                if (SubnodeState.PROCESSING.equals(subnode.getState())) {
                    LOG.debug("------Not steady for follower's {} thread-----" +
                                    "Node {} subnode {} status: {}",
                            subnode.getSubnodeType(), nodeId, subnode.getId(), subnode.getState());
                    return false;
                }
            } else if (SubnodeType.FOLLOWER_PROCESSOR.equals(subnode.getSubnodeType())) {
                followerProcessorExisted = true;
                if (SubnodeState.PROCESSING.equals(subnode.getState())) {
                    LOG.debug("------Not steady for follower's {} thread-----" +
                                    "Node {} subnode {} status: {}",
                            subnode.getSubnodeType(), nodeId, subnode.getId(), subnode.getState());
                    return false;
                }
            }
            if (SubnodeState.PROCESSING.equals(subnode.getState())) {
                LOG.debug("------Not steady for follower's {} thread-----" +
                                "Node {} subnode {} status: {}",
                        subnode.getSubnodeType(), nodeId, subnode.getId(), subnode.getState());
                return false;
            }
            LOG.debug("-----------Follower node {} subnode {} status: {}, subnode type: {}",
                    nodeId, subnode.getId(), subnode.getState(), subnode.getSubnodeType());
        }
        return syncExisted && followerProcessorExisted;
    }
}
