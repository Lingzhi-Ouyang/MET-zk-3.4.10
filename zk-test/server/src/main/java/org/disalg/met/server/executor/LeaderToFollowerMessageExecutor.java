package org.disalg.met.server.executor;

import org.disalg.met.api.*;
import org.disalg.met.server.TestingService;
import org.disalg.met.server.event.LeaderToFollowerMessageEvent;
import org.disalg.met.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class LeaderToFollowerMessageExecutor extends BaseEventExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ElectionMessageExecutor.class);

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
        LOG.debug("Releasing leader message: {}", event.toString());
        releaseLeaderToFollowerMessage(event);
        testingService.waitAllNodesSteady();
        event.setExecuted();
        LOG.debug("Learner handler message executed: {}\n\n\n", event.toString());
        return true;
    }

    /***
     * From leader to follower
     * set sendingSubnode and receivingSubnode SYNC_PROCESSOR / COMMIT_PROCESSOR to PROCESSING
     */
    public void releaseLeaderToFollowerMessage(final LeaderToFollowerMessageEvent event) {
        testingService.setMessageInFlight(event.getId());
        final Subnode sendingSubnode = testingService.getSubnodes().get(event.getSendingSubnodeId());

        // set the sending subnode to be PROCESSING
        sendingSubnode.setState(SubnodeState.PROCESSING);

        testingService.getControlMonitor().notifyAll();

        // if in partition, then just drop it
        final int leaderId = sendingSubnode.getNodeId();
        final int followerId = event.getReceivingNodeId();
        if (testingService.getPartitionMap().get(leaderId).get(followerId)) {
            return;
        }

        // not in partition, so the message can be received
        // set the receiving subnode to be PROCESSING
        final int type = event.getType();
        final NodeState nodeState = testingService.getNodeStates().get(followerId);
        Set<Subnode> subnodes = testingService.getSubnodeSets().get(followerId);

        if (NodeState.ONLINE.equals(nodeState)) {
            switch (type) {
                case MessageType.DIFF:
                case MessageType.TRUNC:
                case MessageType.SNAP:
                    LOG.info("leader sends DIFF / TRUNC / SNAP that follower will not reply : {}", event);
                    break;
                case MessageType.NEWLEADER: // wait for follower's ack to be sent
                    int followerQuorumPeerSubnodeId = -1;
                    for (final Subnode subnode : subnodes) {
                        if (subnode.getSubnodeType().equals(SubnodeType.QUORUM_PEER)
                                && SubnodeState.RECEIVING.equals(subnode.getState())) {
                            // set the receiving QUORUM_PEER subnode to be PROCESSING
                            subnode.setState(SubnodeState.PROCESSING);
                            followerQuorumPeerSubnodeId = subnode.getId();
                            break;
                        }
                    }
                    if (followerQuorumPeerSubnodeId > 0) {
                        testingService.waitSubnodeInSendingState(followerQuorumPeerSubnodeId);
                    } else {
                        LOG.debug("follower {}'s QuorumPeerSubnodeId not found!", followerId);
                    }
                case MessageType.UPTODATE: // wait for follower's ack to be sent
                    for (final Subnode subnode : subnodes) {
                        if (subnode.getSubnodeType().equals(SubnodeType.QUORUM_PEER)
                                && SubnodeState.RECEIVING.equals(subnode.getState())) {
                            // set the receiving QUORUM_PEER subnode to be PROCESSING
                            subnode.setState(SubnodeState.PROCESSING);
                            break;
                        }
                    }
                    break;
                case MessageType.PROPOSAL: // for leader's PROPOSAL in sync, follower will not produce any intercepted event
                    if (Phase.BROADCAST.equals(testingService.getNodePhases().get(followerId))) {
                        for (final Subnode subnode : subnodes) {
                            if ( subnode.getSubnodeType().equals(SubnodeType.SYNC_PROCESSOR)
                                    && SubnodeState.RECEIVING.equals(subnode.getState())) {
                                // set the receiving SYNC_PROCESSOR subnode to be PROCESSING
                                subnode.setState(SubnodeState.PROCESSING);
                                break;
                            }
                        }
                    }
                    break;
                case MessageType.COMMIT: // for leader's COMMIT in sync, follower will not produce any intercepted event
                    if (Phase.BROADCAST.equals(testingService.getNodePhases().get(followerId))) {
                        for (final Subnode subnode : subnodes) {
                            if (subnode.getSubnodeType() == SubnodeType.COMMIT_PROCESSOR
                                    && SubnodeState.RECEIVING.equals(subnode.getState())) {
                                // set the receiving COMMIT_PROCESSOR subnode to be PROCESSING
                                subnode.setState(SubnodeState.PROCESSING);
                                break;
                            }
                        }
                    }
                    break;
                default:
                    LOG.info("leader sends a message : {}", event);
            }
        }

//        if (MessageType.PROPOSAL == type) {
//            for (final Subnode subnode : testingService.getSubnodeSets().get(event.getReceivingNodeId())) {
//                if (subnode.getSubnodeType() == SubnodeType.SYNC_PROCESSOR
//                        && SubnodeState.RECEIVING.equals(subnode.getState())) {
//                    // set the receiving SYNC_PROCESSOR subnode to be PROCESSING
//                    subnode.setState(SubnodeState.PROCESSING);
//                    break;
//                }
//            }
//        } else if (MessageType.COMMIT == type){
//            for (final Subnode subnode : testingService.getSubnodeSets().get(event.getReceivingNodeId())) {
//                if (subnode.getSubnodeType() == SubnodeType.COMMIT_PROCESSOR
//                        && SubnodeState.RECEIVING.equals(subnode.getState())) {
//                    // set the receiving COMMIT_PROCESSOR subnode to be PROCESSING
//                    subnode.setState(SubnodeState.PROCESSING);
//                    break;
//                }
//            }
//        }

//        testingService.getControlMonitor().notifyAll();

        // Post-condition

//        switch (type) {
////            case MessageType.DIFF:
////            case MessageType.TRUNC:
////            case MessageType.SNAP:
//            case MessageType.NEWLEADER:
//            case MessageType.UPTODATE:
//                // wait for the target follower processing local sync event
//                int quorumPeerSubnodeId = -1;
//                if (NodeState.ONLINE.equals(nodeState)) {
//                    for (final Subnode subnode : testingService.getSubnodeSets().get(followerId)) {
//                        if (subnode.getSubnodeType().equals(SubnodeType.QUORUM_PEER)) {
//                            quorumPeerSubnodeId = subnode.getId();
//                        }
//                    }
//                    testingService.waitSubnodeInSendingState(quorumPeerSubnodeId);
//                }
//                break;
//            case MessageType.PROPOSAL:
//            case MessageType.COMMIT:
//                break;
//        }

    }
}
