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

public aspect FollowerAspect {
    private static final Logger LOG = LoggerFactory.getLogger(FollowerAspect.class);

//    private final TestingRemoteService testingService;
//
//    private int myId;
//
//    private int leaderId;
//
//    private Integer lastSentMessageId = null;
//
//    private final Object nodeOnlineMonitor = new Object();
//
//    private boolean proposalLogging = false;
//
//    public FollowerAspect() {
//        try {
//            final Registry registry = LocateRegistry.getRegistry(2599);
//            testingService = (TestingRemoteService) registry.lookup(TestingRemoteService.REMOTE_NAME);
//            LOG.debug("Found the remote testing service.");
//        } catch (final RemoteException e) {
//            LOG.error("Couldn't locate the RMI registry.", e);
//            throw new RuntimeException(e);
//        } catch (final NotBoundException e) {
//            LOG.error("Couldn't bind the testing service.", e);
//            throw new RuntimeException(e);
//        }
//    }
//
//    public int getMyId() {
//        return myId;
//    }
//
//    public TestingRemoteService getTestingService() {
//        return testingService;
//    }
//
//    // Identify the ID of this follower node
//    pointcut setMyId(long id): set(long QuorumPeer.myid) && args(id);
//    after(final long id): setMyId(id) {
//        myId = (int) id;
//        LOG.debug("Set myId = {}", myId);
//    }
//
//    // Identify the ID of the leader node
//    pointcut setLeaderId(): execution(* QuorumPeer.getCurrentVote()) ;
//    after() returning (final Vote vote): setLeaderId() {
//        LOG.debug("I believe the leader is {}", vote.getId());
//        leaderId = (int) vote.getId();
//    }
//
////    pointcut lookForLeader(): execution(* FastLeaderElection.lookForLeader());
////    after() returning (final Vote vote): lookForLeader() {
////        LOG.debug("I believe the leader is {}", vote.getId());
////        leaderId = (int) vote.getId();
////    }
//
//    // Intercept of the proposal logging process when calling FollowerZooKeeperServer.logRequest()
//    pointcut logRequest(org.apache.zookeeper.txn.TxnHeader header, org.apache.jute.Record record):
//            withincode(* Follower.processPacket(..))
//            && call(* FollowerZooKeeperServer.logRequest(org.apache.zookeeper.txn.TxnHeader, org.apache.jute.Record))
//            && args(header, record);
//
//    before(final org.apache.zookeeper.txn.TxnHeader header, final org.apache.jute.Record record):
//            logRequest(header, record) {
//        LOG.debug(". Header: {}, Record: {}", header, record);
//        // We only focus the PROPOSAL type
//        int type = header.getType();
//        if (type != 5) {
//            return;
//        }
//        LOG.debug("Header type == 5, my id: {}, leader id: {}", myId, leaderId);
//        final Set<Integer> predecessorIds = new HashSet<>();
//        try {
//            LOG.debug("Follower {} is about to log a proposal with predecessors {}", myId, predecessorIds.toString());
//            synchronized (nodeOnlineMonitor) {
//                proposalLogging = true;
//            }
//            final String payload = constructProposal(header, record);
//            LOG.debug(payload);
//            lastSentMessageId = testingService.offerProposal(leaderId, myId, payload);
//            LOG.debug("testingService returned id = {}", lastSentMessageId);
//            synchronized (nodeOnlineMonitor) {
//                proposalLogging = false;
//            }
//        } catch (final RemoteException e) {
//            LOG.debug("Encountered a remote exception", e);
//            throw new RuntimeException(e);
//        } catch (final Exception e) {
//            LOG.debug("Uncaught exception", e);
//        }
//    }
//
//    public String constructProposal(final org.apache.zookeeper.txn.TxnHeader header,
//                                    final org.apache.jute.Record record) {
//        return "[PROPOSAL] from leader=" + leaderId +
//                ", to me=" + myId +
//                ", type=setData" +
//                ", content=" + record.toString();
//    }

}
