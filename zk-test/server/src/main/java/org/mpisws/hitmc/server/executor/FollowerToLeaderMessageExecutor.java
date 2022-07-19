package org.mpisws.hitmc.server.executor;

import org.mpisws.hitmc.api.MessageType;
import org.mpisws.hitmc.api.Phase;
import org.mpisws.hitmc.api.SubnodeState;
import org.mpisws.hitmc.api.SubnodeType;
import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.event.FollowerToLeaderMessageEvent;
import org.mpisws.hitmc.server.state.Subnode;
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

        // if in partition, then just drop it
        final int followerId = sendingSubnode.getNodeId();
        final int leaderId = event.getReceivingNodeId();
        if (testingService.getPartitionMap().get(followerId).get(leaderId)) {
            return;
        }

        // not int partition
        final int type = event.getType();
        List<Set<Subnode>> subnodeSets = testingService.getSubnodeSets();
        if (MessageType.PROPOSAL == type) {
            for (final Subnode subnode : subnodeSets.get(event.getReceivingNodeId())) {
                if (subnode.getSubnodeType() == SubnodeType.SYNC_PROCESSOR
                        && SubnodeState.RECEIVING.equals(subnode.getState())) {
                    // set the receiving subnode to be PROCESSING
                    subnode.setState(SubnodeState.PROCESSING);
                    break;
                }
            }
        } else if (MessageType.COMMIT == type){
            for (final Subnode subnode : subnodeSets.get(event.getReceivingNodeId())) {
                if (subnode.getSubnodeType() == SubnodeType.COMMIT_PROCESSOR
                        && SubnodeState.RECEIVING.equals(subnode.getState())) {
                    // set the receiving subnode to be PROCESSING
                    subnode.setState(SubnodeState.PROCESSING);
                    break;
                }
            }
        }
        testingService.getControlMonitor().notifyAll();

        // Post-condition

        // this describes the message type that this ACK replies to
        if (type == MessageType.NEWLEADER) {
            // wait for the leader send UPTODATE
            final Phase followerPhase = testingService.getNodePhases().get(followerId);
            if (testingService.getFollowerLearnerHandlerSenderMap().get(followerId) == null) {
                testingService.waitFollowerMappingLearnerHandlerSender(followerId);
            }
            final int learnerHanlderSubnodId = testingService.getFollowerLearnerHandlerSenderMap().get(followerId);
            // distinguish UPTODATE / COMMIT by checking the follower phase sync / broadcast
            if (followerPhase.equals(Phase.SYNC)) {
                // get according learnerHandlerSender
                testingService.waitSubnodeInSendingState(learnerHanlderSubnodId);
            }
        } else if (type == MessageType.UPTODATE) {
            testingService.waitFollowerSteadyAfterProcessingUPTODATE(followerId);
        }
        else {
            LOG.info("follower replies to previous message type : {}", type);
        }
        testingService.waitAllNodesSteady();
    }
}
