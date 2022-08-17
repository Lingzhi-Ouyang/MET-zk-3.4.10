package org.disalg.met.server.executor;

import org.disalg.met.api.*;
import org.disalg.met.server.TestingService;
import org.disalg.met.server.event.FollowerToLeaderMessageEvent;
import org.disalg.met.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class FollowerToLeaderMessageExecutor extends BaseEventExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ElectionMessageExecutor.class);

    private final TestingService testingService;

    public FollowerToLeaderMessageExecutor(final TestingService testingService) {
        this.testingService = testingService;
    }

    @Override
    public boolean execute(final FollowerToLeaderMessageEvent event) throws IOException {
        if (event.isExecuted()) {
            LOG.info("Skipping an executed follower message event: {}", event.toString());
            return false;
        }
        LOG.debug("Releasing message: {}", event.toString());
        releaseFollowerToLeaderMessage(event);
        testingService.waitAllNodesSteady();
        event.setExecuted();
        LOG.debug("Follower message event executed: {}\n\n\n", event.toString());
        return true;
    }

    /***
     * For message events from follower to leader
     * set sendingSubnode and receivingSubnode to PROCESSING
     */
    public void releaseFollowerToLeaderMessage(final FollowerToLeaderMessageEvent event) {
        testingService.setMessageInFlight(event.getId());
        final Subnode sendingSubnode = testingService.getSubnodes().get(event.getSendingSubnodeId());

        // set the sending subnode to be PROCESSING
        sendingSubnode.setState(SubnodeState.PROCESSING);

        testingService.getControlMonitor().notifyAll();

        // if in partition, then just drop it
        final int followerId = sendingSubnode.getNodeId();
        final int leaderId = event.getReceivingNodeId();
        LOG.debug("partition map: {}, follower: {}, leader: {}", testingService.getPartitionMap(), followerId, leaderId);
        if (testingService.getPartitionMap().get(followerId).get(leaderId)) {
            return;
        }

        // not in partition, so the message can be received
        // set the receiving subnode to be PROCESSING
        final int type = event.getType();
        final NodeState nodeState = testingService.getNodeStates().get(leaderId);
        Set<Subnode> subnodes = testingService.getSubnodeSets().get(leaderId);
        final Phase followerPhase = testingService.getNodePhases().get(followerId);

        if (NodeState.ONLINE.equals(nodeState)) {
            switch (type) {
                case MessageType.NEWLEADER:
                    if (testingService.getFollowerLearnerHandlerSenderMap().get(followerId) == null) {
                        testingService.waitFollowerMappingLearnerHandlerSender(followerId);
                    }
                    final int learnerHanlderSubnodId = testingService.getFollowerLearnerHandlerSenderMap().get(followerId);
                    // distinguish the follower phase sync / broadcast
                    if (followerPhase.equals(Phase.SYNC)) {
                        for (final Subnode subnode : subnodes) {
                            if (subnode.getId() == learnerHanlderSubnodId
                                    && SubnodeState.RECEIVING.equals(subnode.getState())) {
                                // set the receiving LEARNER_HANDLER subnode to be PROCESSING
                                subnode.setState(SubnodeState.PROCESSING);
                                break;
                            }
                        }
                        // get according learnerHandlerSender
//                        testingService.waitSubnodeInSendingState(learnerHanlderSubnodId);
                    }
                    break;
                case MessageType.UPTODATE:
                    testingService.waitFollowerSteadyAfterProcessingUPTODATE(followerId);
                    break;
                case MessageType.PROPOSAL: // for now we do not intercept leader's PROPOSAL in sync
                    // make the learner handler PROCESSING
                    LOG.info("follower replies to previous PROPOSAL message type : {}", event);
                    final int learnerHanlderSubnodId2 = testingService.getFollowerLearnerHandlerSenderMap().get(followerId);
                    testingService.waitSubnodeInSendingState(learnerHanlderSubnodId2);
//                    final long zxid = event.getZxid();
//                    if (quorumSynced(zxid)){
//                        testingService.waitAllNodesSteadyAfterQuorumSynced(zxid);
//                        if (testingService.getFollowerLearnerHandlerSenderMap().get(followerId) == null) {
//                            testingService.waitFollowerMappingLearnerHandlerSender(followerId);
//                        }
//                        final int learnerHanlderSubnodId2 = testingService.getFollowerLearnerHandlerSenderMap().get(followerId);
//                        // distinguish the follower phase sync / broadcast
//                        if (followerPhase.equals(Phase.BROADCAST)) {
//                            for (final Subnode subnode : subnodes) {
//                                if (subnode.getId() == learnerHanlderSubnodId2
//                                        && SubnodeState.RECEIVING.equals(subnode.getState())) {
//                                    // set the receiving LEARNER_HANDLER subnode to be PROCESSING
//                                    subnode.setState(SubnodeState.PROCESSING);
//                                    break;
//                                }
//                            }
//                        }
//                    }
                    break;
                case MessageType.COMMIT: // for now we do not intercept leader's COMMIT in sync
                    LOG.info("follower replies to previous COMMIT message type : {}", event);
                    break;
                case MessageType.LEADERINFO:
                    // wait for leader update currentEpoch file
                    LOG.info("follower replies ACKEPOCH : {}", event);
                    testingService.waitCurrentEpochUpdated(leaderId);
                    break;
                default:
                    LOG.info("follower replies to previous message type : {}", event);
            }
        }

//        if (MessageType.PROPOSAL == type) {
//            for (final Subnode subnode : subnodes) {
//                if (subnode.getSubnodeType() == SubnodeType.SYNC_PROCESSOR
//                        && SubnodeState.RECEIVING.equals(subnode.getState())) {
//                    // set the receiving subnode to be PROCESSING
//                    subnode.setState(SubnodeState.PROCESSING);
//                    break;
//                }
//            }
//        } else if (MessageType.COMMIT == type){
//            for (final Subnode subnode : subnodes) {
//                if (subnode.getSubnodeType() == SubnodeType.COMMIT_PROCESSOR
//                        && SubnodeState.RECEIVING.equals(subnode.getState())) {
//                    // set the receiving subnode to be PROCESSING
//                    subnode.setState(SubnodeState.PROCESSING);
//                    break;
//                }
//            }
//        }

//        testingService.getControlMonitor().notifyAll();

        // Post-condition

//        // this describes the message type that this ACK replies to
//        if (type == MessageType.NEWLEADER) {
//            // wait for the leader send UPTODATE
//            final Phase followerPhase = testingService.getNodePhases().get(followerId);
//            if (testingService.getFollowerLearnerHandlerSenderMap().get(followerId) == null) {
//                testingService.waitFollowerMappingLearnerHandlerSender(followerId);
//            }
//            final int learnerHanlderSubnodId = testingService.getFollowerLearnerHandlerSenderMap().get(followerId);
//            // distinguish UPTODATE / COMMIT by checking the follower phase sync / broadcast
//            if (followerPhase.equals(Phase.SYNC)) {
//                // get according learnerHandlerSender
//                testingService.waitSubnodeInSendingState(learnerHanlderSubnodId);
//            }
//        } else if (type == MessageType.UPTODATE) {
//            testingService.waitFollowerSteadyAfterProcessingUPTODATE(followerId);
//        }
//        else {
//            LOG.info("follower replies to previous message type : {}", type);
//        }

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
