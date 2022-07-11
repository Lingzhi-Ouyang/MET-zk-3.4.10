package org.apache.zookeeper.server.quorum;

import org.mpisws.hitmc.api.SubnodeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;

/***
 * ensure this is executed by the QuorumPeer thread
 */
public aspect LearnerAspect {
    private static final Logger LOG = LoggerFactory.getLogger(LearnerAspect.class);

    private final QuorumPeerAspect quorumPeerAspect = QuorumPeerAspect.aspectOf();

    /***
     * For follower's sync with leader process without replying (partition will not work)
     *  --> FollowerProcessSyncMessage : will not send anything.
     *  --> FollowerProcessPROPOSALInSync :  will not send anything. pass for now
     *  --> FollowerProcessCOMMITInSync : will not send anything. pass for now
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
        if (type == Leader.DIFF || type == Leader.TRUNC || type == Leader.SNAP) {
            try {
                // before offerMessage: increase sendingSubnodeNum
                quorumPeerAspect.setSubnodeSending();
                final long zxid = packet.getZxid();
                final int followerReadPacketId =
                        quorumPeerAspect.getTestingService().offerLocalEvent(quorumPeerSubnodeId, SubnodeType.QUORUM_PEER, zxid, payload);
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
     *  --> FollowerProcessNEWLEADER
     *  --> FollowerProcessUPTODATE
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
            // FollowerProcessNEWLEADER
            LOG.debug("Follower is about to reply a message to leader which is not an ACK. (type={})", type);
            proceed(packet, flush);
            return;
        }
        if (quorumPeerAspect.isNewLeaderDone()) {
            // FollowerProcessUPTODATE
            try {
                quorumPeerAspect.setSyncFinished(true);
                quorumPeerAspect.getTestingService().readyForBroadcast(quorumPeerSubnodeId);
                LOG.debug("-------receiving UPTODATE!!!!-------begin to serve clients");
                proceed(packet, flush);
                return;
            } catch (RemoteException e) {
                LOG.debug("Encountered a remote exception", e);
                throw new RuntimeException(e);
            }

        } else {
            // processing Leader.NEWLEADER
            try {
                quorumPeerAspect.setSyncFinished(false);
                quorumPeerAspect.setNewLeaderDone(true);
                LOG.debug("-------receiving NEWLEADER!!!!-------reply ACK");
                final long zxid = packet.getZxid();
                quorumPeerAspect.getTestingService().offerFollowerMessageToLeader(quorumPeerSubnodeId, zxid, payload, type);

                // TODO: partition

                proceed(packet, flush);
                return;
            } catch (RemoteException e) {
                LOG.debug("Encountered a remote exception", e);
                throw new RuntimeException(e);
            }
        }
//        if (!quorumPeerAspect.isSyncFinished()){
//            LOG.debug("-------still in sync!");
//            return;
//        }
    }
}
