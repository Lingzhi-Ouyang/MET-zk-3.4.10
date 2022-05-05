package org.apache.zookeeper.server.quorum;

import org.mpisws.hitmc.api.TestingRemoteService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashSet;
import java.util.Set;

public aspect LearnerAspect {
    private static final Logger LOG = LoggerFactory.getLogger(LearnerAspect.class);

    private final QuorumPeerAspect quorumPeerAspect = QuorumPeerAspect.aspectOf();

    /***
     * For follower's sync with leader process
     * Related code: Learner.java
     */
    pointcut readPacketInSyncWithLeader(QuorumPacket packet):
            withincode(* org.apache.zookeeper.server.quorum.Learner.syncWithLeader(..)) &&
                    call(void org.apache.zookeeper.server.quorum.Learner.readPacket(QuorumPacket)) && args(packet);

    after(QuorumPacket packet) returning: readPacketInSyncWithLeader(packet) {
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("after receiveUPTODATE-------Thread: {}, {}------", threadId, threadName);

        final String payload = quorumPeerAspect.packetToString(packet);
        final int quorumPeerSubnodeId = quorumPeerAspect.getQuorumPeerSubnodeId();
        LOG.debug("---------readPacket: ({}). Subnode: {}", payload, quorumPeerSubnodeId);
        final int type =  packet.getType();
        if (type == Leader.UPTODATE) {
            try {
                quorumPeerAspect.setSyncFinished(true);
                quorumPeerAspect.getTestingService().readyForBroadcast(quorumPeerSubnodeId);
                LOG.debug("-------receiving UPTODATE!!!!-------begin to serve clients");
            } catch (RemoteException e) {
                LOG.debug("Encountered a remote exception", e);
                throw new RuntimeException(e);
            }
        }
        if (!quorumPeerAspect.isSyncFinished()){
            LOG.debug("-------still in sync!");
            return;
        }
    }
}
