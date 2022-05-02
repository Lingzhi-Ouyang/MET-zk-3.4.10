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

public aspect FollowerZooKeeperServerAspect {
    private static final Logger LOG = LoggerFactory.getLogger(FollowerZooKeeperServerAspect.class);

//    private final TestingRemoteService testingService;
//
////    private int myId;
//
//    private Integer lastLoggedMessageId = null;
//
//    private final Object nodeLoggingMonitor = new Object();
//
//    private boolean proposalSending = false;
//
//    public FollowerZooKeeperServerAspect() {
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
////    public int getMyId() {
////        return myId;
////    }
////
////    // Identify the ID of this node
////
////    pointcut setMyId(long id): set(long QuorumPeer.myid) && args(id);
////
////    after(final long id): setMyId(id) {
////        myId = (int) id;
////        LOG.debug("Set myId = {}", myId);
////    }
//
//
//    // Intercept FollowerZooKeeperServer.logRequest()
//
//    pointcut logRequest(Object header, Object record):
//            execution(* FollowerZooKeeperServer.logRequest(org.apache.zookeeper.txn.TxnHeader, org.apache.jute.Record))
//            && if (header instanceof org.apache.zookeeper.txn.TxnHeader)
//            && if (record instanceof org.apache.jute.Record)
//            && args(header, record);
//
//    // TODO: only intercept PROPOSAL OF SET_DATA TYPE
//    // TODO: get FOLLOWER NODE ID
//    before(final Object header, final Object record):
//            logRequest(header, record) {
//        final Set<Integer> predecessorIds = new HashSet<>();
//        LOG.debug("Before logRequest in FollowerZooKeeperServer. Header: {}, Record: {}", header, record);
////        try {
////            synchronized (nodeLoggingMonitor) {
////                proposalSending = true;
////            }
////            final String payload = constructPayload();
////            lastLoggedMessageId = scheduler.offerProposal(payload);
////            LOG.debug("Scheduler returned proposal message id = {}", lastLoggedMessageId);
////            synchronized (nodeLoggingMonitor) {
////                proposalSending = false;
////            }
////        } catch (final RemoteException e) {
////            LOG.debug("Encountered a remote exception", e);
////            throw new RuntimeException(e);
////        } catch (final Exception e) {
////            LOG.debug("Uncaught exception", e);
////        }
//    }
//
//
//    public String constructPayload() {
//        return "PROPOSAL";
//    }

}
