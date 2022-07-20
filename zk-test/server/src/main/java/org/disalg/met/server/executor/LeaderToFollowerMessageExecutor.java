package org.disalg.met.server.executor;

import org.disalg.met.api.SubnodeState;
import org.disalg.met.api.MessageType;
import org.disalg.met.api.NodeState;
import org.disalg.met.api.SubnodeType;
import org.disalg.met.server.TestingService;
import org.disalg.met.server.event.LeaderToFollowerMessageEvent;
import org.disalg.met.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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

        // if in partition, then just drop it
        final int sendingNodeId = sendingSubnode.getNodeId();
        final int receivingNodeId = event.getReceivingNodeId();
        if (testingService.getPartitionMap().get(sendingNodeId).get(receivingNodeId)) {
            return;
        }

        // not int partition
        // not in sync
        final int type = event.getType();
        if (MessageType.PROPOSAL == type) {
            for (final Subnode subnode : testingService.getSubnodeSets().get(event.getReceivingNodeId())) {
                if (subnode.getSubnodeType() == SubnodeType.SYNC_PROCESSOR
                        && SubnodeState.RECEIVING.equals(subnode.getState())) {
                    // set the receiving subnode to be PROCESSING
                    subnode.setState(SubnodeState.PROCESSING);
                    break;
                }
            }
        } else if (MessageType.COMMIT == type){
            for (final Subnode subnode : testingService.getSubnodeSets().get(event.getReceivingNodeId())) {
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
        final int followerId = event.getReceivingNodeId();
        final NodeState nodeState = testingService.getNodeStates().get(followerId);

        switch (type) {
//            case MessageType.DIFF:
//            case MessageType.TRUNC:
//            case MessageType.SNAP:
            case MessageType.NEWLEADER:
            case MessageType.UPTODATE:
                // wait for the target follower processing local sync event
                int quorumPeerSubnodeId = -1;
                if (NodeState.ONLINE.equals(nodeState)) {
                    for (final Subnode subnode : testingService.getSubnodeSets().get(followerId)) {
                        if (subnode.getSubnodeType().equals(SubnodeType.QUORUM_PEER)) {
                            quorumPeerSubnodeId = subnode.getId();
                        }
                    }
                    testingService.waitSubnodeInSendingState(quorumPeerSubnodeId);
                }
                break;
            case MessageType.PROPOSAL:
            case MessageType.COMMIT:
                break;
        }
        testingService.waitAllNodesSteady();
    }
}
