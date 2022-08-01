package org.apache.zookeeper.server.quorum;

import org.disalg.met.api.SubnodeType;
import org.disalg.met.api.TestingDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;

/***
 * ensure this is executed by the QuorumPeer thread
 */
public aspect LearnerAspect {
    private static final Logger LOG = LoggerFactory.getLogger(LearnerAspect.class);

    private final QuorumPeerAspect quorumPeerAspect = QuorumPeerAspect.aspectOf();

    //Since follower will always reply ACK type, so it is more useful to match its last package type
    private int lastReadType = -1;

    /***
     * For follower's sync with leader process without replying (partition will not work)
     * For getting lastReadType during SYNC:
     *  DIFF / TRUNC / SNAP --> FollowerProcessSyncMessage : will not send ACK.
     *  PROPOSAL --> FollowerProcessPROPOSALInSync :  will not send ACK during sync. Actually will send ACK until in broadcast phase. see zk-3911
     *  COMMIT --> FollowerProcessCOMMITInSync : will not send ACK.
     *  NEWLEADER --> record this message type, will send ACK, so will be processed in writePacketInSyncWithLeader
     *  UPTODATE --> record this message type, will send ACK, so will be processed in writePacketInSyncWithLeader
     * Related code: Learner.java
     */
    pointcut readPacketInSyncWithLeader(QuorumPacket packet):
            withincode(* org.apache.zookeeper.server.quorum.Learner.syncWithLeader(..)) &&
                    call(void org.apache.zookeeper.server.quorum.Learner.readPacket(QuorumPacket)) && args(packet);

    after(QuorumPacket packet) returning: readPacketInSyncWithLeader(packet) {
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("follower readPacketInSyncWithLeader-------Thread: {}, {}------", threadId, threadName);

        final String payload = quorumPeerAspect.packetToString(packet);
        final int quorumPeerSubnodeId = quorumPeerAspect.getQuorumPeerSubnodeId();

        // Set RECEIVING state since there is nowhere else to set
        try {
            quorumPeerAspect.getTestingService().setReceivingState(quorumPeerSubnodeId);
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }


        LOG.debug("---------readPacket: ({}). Subnode: {}", payload, quorumPeerSubnodeId);
        final int type =  packet.getType();
        lastReadType = type;
        // will not intercept NEWLEADER / UPTODATE
        if (type == Leader.DIFF || type == Leader.TRUNC || type == Leader.SNAP
                || type == Leader.PROPOSAL || type == Leader.COMMIT) {
            try {
                // before offerMessage: increase sendingSubnodeNum
                quorumPeerAspect.setSubnodeSending();
                final long zxid = packet.getZxid();
                final int followerReadPacketId =
                        quorumPeerAspect.getTestingService().offerLocalEvent(quorumPeerSubnodeId, SubnodeType.QUORUM_PEER, zxid, payload, type);
                LOG.debug("followerReadPacketId = {}", followerReadPacketId);
                // after offerMessage: decrease sendingSubnodeNum and shutdown this node if sendingSubnodeNum == 0
                quorumPeerAspect.postSend(quorumPeerSubnodeId, followerReadPacketId);

                // Trick: set RECEIVING state here
                quorumPeerAspect.getTestingService().setReceivingState(quorumPeerSubnodeId);

            } catch (RemoteException e) {
                e.printStackTrace();
            }
        }
    }

    /***
     * For follower's sync with leader process with sending REPLY (partition will work on the process)
     * Since follower will always reply ACK type, so it is more useful to match its last package type
     *  lastReadType==NEWLEADER --> FollowerProcessUPTODATE : send ACK to UPTODATE, offerFollowerToLeaderMessage
     *  lastReadType==UPTODATE --> FollowerProcessNEWLEADER : send ACK to NEWLEADER,  offerFollowerToLeaderMessage
     * Related code: Learner.java
     */
    pointcut writePacketInSyncWithLeader(QuorumPacket packet, boolean flush):
            withincode(* org.apache.zookeeper.server.quorum.Learner.syncWithLeader(..)) &&
                    call(void org.apache.zookeeper.server.quorum.Learner.writePacket(QuorumPacket, boolean)) && args(packet, flush);

    void around(QuorumPacket packet, boolean flush): writePacketInSyncWithLeader(packet, flush) {
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("follower writePacketInSyncWithLeader-------Thread: {}, {}------", threadId, threadName);

        final String payload = quorumPeerAspect.packetToString(packet);
        final int quorumPeerSubnodeId = quorumPeerAspect.getQuorumPeerSubnodeId();
        LOG.debug("---------writePacket: ({}). Subnode: {}", payload, quorumPeerSubnodeId);
        final int type =  packet.getType();
        if (type != Leader.ACK) {
            LOG.debug("Follower is about to reply a message to leader which is not an ACK. (type={})", type);
            proceed(packet, flush);
            return;
        }

//        if (quorumPeerAspect.isNewLeaderDone()) {
        if (lastReadType == Leader.UPTODATE) {
            // FollowerProcessUPTODATE
            try {
                LOG.debug("-------receiving UPTODATE!!!!-------begin to serve clients");

                quorumPeerAspect.setSubnodeSending();
                final long zxid = packet.getZxid();
                final int followerWritePacketId = quorumPeerAspect.getTestingService().offerFollowerToLeaderMessage(quorumPeerSubnodeId, zxid, payload, lastReadType);

                // after offerMessage: decrease sendingSubnodeNum and shutdown this node if sendingSubnodeNum == 0
                quorumPeerAspect.postSend(quorumPeerSubnodeId, followerWritePacketId);

                quorumPeerAspect.setSyncFinished(true);
                quorumPeerAspect.getTestingService().readyForBroadcast(quorumPeerSubnodeId);

                // Trick: set RECEIVING state here
                quorumPeerAspect.getTestingService().setReceivingState(quorumPeerSubnodeId);

                // to check if the partition happens
                if (followerWritePacketId == TestingDef.RetCode.NODE_PAIR_IN_PARTITION){
                    // just drop the message
                    LOG.debug("partition occurs! just drop the message. What about other types of messages?");
                    return;
                }

                proceed(packet, flush);
                return;
            } catch (RemoteException e) {
                LOG.debug("Encountered a remote exception", e);
                throw new RuntimeException(e);
            }
        } else if (lastReadType == Leader.NEWLEADER) {
            // processing Leader.NEWLEADER
            try {
                quorumPeerAspect.setSyncFinished(false);
                quorumPeerAspect.setNewLeaderDone(true);
                LOG.debug("-------receiving NEWLEADER!!!!-------reply ACK");
                quorumPeerAspect.setSubnodeSending();
                final long zxid = packet.getZxid();
                final int followerWritePacketId = quorumPeerAspect.getTestingService().offerFollowerToLeaderMessage(quorumPeerSubnodeId, zxid, payload, lastReadType);

                // after offerMessage: decrease sendingSubnodeNum and shutdown this node if sendingSubnodeNum == 0
                quorumPeerAspect.postSend(quorumPeerSubnodeId, followerWritePacketId);

                // Trick: set RECEIVING state here
                quorumPeerAspect.getTestingService().setReceivingState(quorumPeerSubnodeId);

                // to check if the partition happens
                if (followerWritePacketId == TestingDef.RetCode.NODE_PAIR_IN_PARTITION){
                    // just drop the message
                    LOG.debug("partition occurs! just drop the message. What about other types of messages?");
                    return;
                }

                proceed(packet, flush);
                return;
            } catch (RemoteException e) {
                LOG.debug("Encountered a remote exception", e);
                throw new RuntimeException(e);
            }
        } else {
            proceed(packet, flush);
            return;
        }
    }
}
