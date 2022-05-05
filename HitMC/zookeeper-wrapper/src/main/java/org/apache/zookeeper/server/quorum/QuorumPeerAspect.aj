package org.apache.zookeeper.server.quorum;

import org.apache.jute.Record;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.txn.TxnHeader;
import org.mpisws.hitmc.api.TestingDef;
import org.mpisws.hitmc.api.TestingRemoteService;
import org.mpisws.hitmc.api.SubnodeType;
import org.mpisws.hitmc.api.state.LeaderElectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketOptions;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/***
 * The main thread for a node. (corresponding to the QuorumPeer thread in ZooKeeper)
 * All other enhanced threads will call this class' methods when:
 * -> 1. the thread starts to run (registerXXXSubnode)
 * -> 2. the thread is about to exit (deregisterXXXSubnode)
 * -> 3. the thread is about to send / process a message (setXXXSending)
 * -> 4. the thread is allowed to send / process the message (XXXPostSend)
 *      --> Actually, the message is still not sent until this method is done
 *      --> If the msgId == -1, the last existing subnode needs to notify nodeOffline
 * Above methods need to be implemented using the shared variable nodeOnlineMonitor with the critical section
 *
 * The following methods need to use the nodeId
 * -> 1. the thread is about to construct a message payload (constructXXX)
 * ->
 */
public aspect QuorumPeerAspect {

    private static final Logger LOG = LoggerFactory.getLogger(QuorumPeerAspect.class);

    private final TestingRemoteService testingService;

    private int myId;

    private Socket mySock; // for follower

    private boolean syncFinished = false; // for follower

    private Integer lastSentMessageId = null;

    private FastLeaderElection.Notification notification;

    private int quorumPeerSubnodeId;

    private final Object nodeOnlineMonitor = new Object();

    // Manage uncertain number of subnodes
    private boolean quorumPeerSubnodeRegistered = false;
    private boolean workerReceiverSubnodeRegistered = false;

    // 1. Use variables for specific subnodes
//    private boolean quorumPeerSending = false;
//    private boolean workerReceiverSending = false;
//    private boolean syncProcessorSending = false;
//    private final Map<Integer, Boolean> learnerHandlerSendingMap = new HashMap<>();

    // 2. Use map for all subnodes
//    private final Map<Integer, Boolean> subnodeSendingMap = new HashMap<>();

    // 3. Use a counter
    private final AtomicInteger sendingSubnodeNum = new AtomicInteger(0);

    // Maintain a subnode list
    private final Map<Long, SubnodeIntercepter> intercepterMap = new HashMap<>();

    public static class SubnodeIntercepter {
        private String threadName;

        private int subnodeId;

        private SubnodeType subnodeType;

        private TestingRemoteService testingService;

        private Integer lastMsgId = null;

        // This is for learner handler sender. After a learner handler has sent UPTODATE, then set it true
        private boolean syncFinished = false;

        @Deprecated
        private final AtomicInteger msgsInQueue = new AtomicInteger(0);

        public SubnodeIntercepter(String threadName, int subnodeId, SubnodeType subnodeType, TestingRemoteService testingService){
            this.threadName = threadName;
            this.subnodeId = subnodeId;
            this.subnodeType = subnodeType;
            this.testingService = testingService;
        }

        public int getSubnodeId() {
            return subnodeId;
        }

        public SubnodeType getSubnodeType() {
            return subnodeType;
        }

        public TestingRemoteService getTestingService() {
            return testingService;
        }

        public AtomicInteger getMsgsInQueue() {
            return msgsInQueue;
        }

        public void setLastMsgId(Integer lastMsgId) {
            this.lastMsgId = lastMsgId;
        }

        public void setSyncFinished(boolean syncFinished) {
            this.syncFinished = syncFinished;
        }

        public boolean isSyncFinished() {
            return syncFinished;
        }
    }

    public SubnodeIntercepter getIntercepter(long threadId) {
        return intercepterMap.get(threadId);
    }

    public TestingRemoteService createRmiConnection() {
        try {
            final Registry registry = LocateRegistry.getRegistry(2599);
            return (TestingRemoteService) registry.lookup(TestingRemoteService.REMOTE_NAME);
        } catch (final RemoteException e) {
            LOG.error("Couldn't locate the RMI registry.", e);
            throw new RuntimeException(e);
        } catch (final NotBoundException e) {
            LOG.error("Couldn't bind the testing service.", e);
            throw new RuntimeException(e);
        }
    }

    public QuorumPeerAspect() {
        try {
            final Registry registry = LocateRegistry.getRegistry(2599);
            testingService = (TestingRemoteService) registry.lookup(TestingRemoteService.REMOTE_NAME);
            LOG.debug("Found the remote testing service.");
        } catch (final RemoteException e) {
            LOG.error("Couldn't locate the RMI registry.", e);
            throw new RuntimeException(e);
        } catch (final NotBoundException e) {
            LOG.error("Couldn't bind the testing service.", e);
            throw new RuntimeException(e);
        }
    }

    public int getMyId() {
        return myId;
    }

    public int getQuorumPeerSubnodeId() {
        return quorumPeerSubnodeId;
    }

    public TestingRemoteService getTestingService() {
        return testingService;
    }

    public void setSyncFinished(boolean syncFinished) {
        this.syncFinished = syncFinished;
    }

    public boolean isSyncFinished() {
        return syncFinished;
    }

    // Identify the ID of this node

    pointcut setMyId(long id): set(long org.apache.zookeeper.server.quorum.QuorumPeer.myid) && args(id);

    after(final long id): setMyId(id) {
        myId = (int) id;
        LOG.debug("Set myId = {}", myId);
    }

    // Intercept starting the QuorumPeer thread

    pointcut runQuorumPeer(): execution(* QuorumPeer.run());

    before(): runQuorumPeer() {
        try {
            LOG.debug("-------Thread: {}------", Thread.currentThread().getName());
            LOG.debug("----------------Registering QuorumPeer subnode");
            quorumPeerSubnodeId = testingService.registerSubnode(myId, SubnodeType.QUORUM_PEER);
            LOG.debug("Registered QuorumPeer subnode: id = {}", quorumPeerSubnodeId);
            synchronized (nodeOnlineMonitor) {
                quorumPeerSubnodeRegistered = true;
                if (workerReceiverSubnodeRegistered) {
                    testingService.nodeOnline(myId);
                }
            }
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    after(): runQuorumPeer() {
        try {
            LOG.debug("De-registering QuorumPeer subnode");
            testingService.deregisterSubnode(quorumPeerSubnodeId);
            LOG.debug("-------------------De-registered QuorumPeer subnode\n-------------\n");
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    // Intercept FastLeaderElection.lookForLeader()

    pointcut lookForLeader(): execution(* org.apache.zookeeper.server.quorum.FastLeaderElection.lookForLeader());

    after() returning (final Vote vote): lookForLeader() {
        try {
            testingService.updateVote(myId, constructVote(vote));
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    // Intercept message offering within FastLeaderElection, but not within WorkerReceiver

    pointcut offerWithinFastLeaderElection(Object object):
            within(org.apache.zookeeper.server.quorum.FastLeaderElection) && !within(org.apache.zookeeper.server.quorum.FastLeaderElection.Messenger.WorkerReceiver)
            && call(* LinkedBlockingQueue.offer(java.lang.Object))
            && if (object instanceof FastLeaderElection.ToSend)
            && args(object);

    before(final Object object): offerWithinFastLeaderElection(object) {
        final FastLeaderElection.ToSend toSend = (FastLeaderElection.ToSend) object;

        final Set<Integer> predecessorIds = new HashSet<>();
        if (null != notification) {
            predecessorIds.add(notification.getMessageId());
        }
        if (null != lastSentMessageId) {
            predecessorIds.add(lastSentMessageId);
        }

        try {
            LOG.debug("QuorumPeer subnode {} is offering a message with predecessors {}", quorumPeerSubnodeId, predecessorIds.toString());
//            synchronized (nodeOnlineMonitor) {
////                quorumPeerSending = true;
//                subnodeSendingMap.put(quorumPeerSubnodeId, true);
//            }
            setSubnodeSending();
            final String payload = constructPayload(toSend);
            lastSentMessageId = testingService.offerMessage(quorumPeerSubnodeId, (int) toSend.sid, predecessorIds, payload);
            LOG.debug("lastSentMessageId = {}", lastSentMessageId);
            postSend(quorumPeerSubnodeId, lastSentMessageId);
//            synchronized (nodeOnlineMonitor) {
//                quorumPeerSending = false;
//                if (lastSentMessageId == -1) {
//                    // The last existing subnode is responsible to set the node state as offline
//                    if (!workerReceiverSending && !syncProcessorSending) {
//                        testingService.nodeOffline(myId);
//                    }
//                    awaitShutdown(quorumPeerSubnodeId);
//                }
//            }
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        } catch (final Exception e) {
            LOG.debug("Uncaught exception", e);
        }
    }

//    public void setQuorumPeerSending(final int subnodeId) {
//        synchronized (nodeOnlineMonitor) {
////            quorumPeerSending = true;
////            subnodeSendingMap.put(subnodeId, true);
//            sendingSubnodeNum.incrementAndGet();
//        }
//    }

//    public void quorumPeerPostSend(final int subnodeId, final int msgId) throws RemoteException {
//        synchronized (nodeOnlineMonitor) {
////            quorumPeerSending = false;
////            subnodeSendingMap.put(subnodeId, false);
//            sendingSubnodeNum.decrementAndGet();
//            if (lastSentMessageId == -1) {
//                // The last existing subnode is responsible to set the node state as offline
////                if (!workerReceiverSending && !syncProcessorSending) {
//                if (sendingSubnodeNum.get() == 0) {
//                    testingService.nodeOffline(myId);
//                }
//                awaitShutdown(quorumPeerSubnodeId);
//            }
//        }
//    }

    public void setSubnodeSending() {
        sendingSubnodeNum.incrementAndGet();
    }

    public void postSend(final int subnodeId, final int msgId) throws RemoteException {
        sendingSubnodeNum.decrementAndGet();
        if (msgId == TestingDef.RetCode.NODE_CRASH) {
            // The last existing subnode is responsible to set the node state as offline
            if (sendingSubnodeNum.get() == 0) {
                testingService.nodeOffline(myId);
            }
            awaitShutdown(subnodeId);
        }
    }

    public void awaitShutdown(final int subnodeId) {
        try {
            LOG.debug("De-registering subnode {}", subnodeId);
            testingService.deregisterSubnode(subnodeId);
            // Going permanently to the wait queue
            nodeOnlineMonitor.wait();
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        } catch (final InterruptedException e) {
            LOG.debug("Interrupted from waiting on nodeOnlineMonitor", e);
        }

    }

    // Intercept polling the FastLeaderElection.recvqueue

    pointcut pollRecvQueue(LinkedBlockingQueue queue):
            withincode(* org.apache.zookeeper.server.quorum.FastLeaderElection.lookForLeader())
            && call(* LinkedBlockingQueue.poll(..))
            && target(queue);

    before(final LinkedBlockingQueue queue): pollRecvQueue(queue) {
        if (queue.isEmpty()) {
            LOG.debug("My FLE.recvqueue is empty, go to RECEIVING state");
            // Going to block here
            try {
                testingService.setReceivingState(quorumPeerSubnodeId);
            } catch (final RemoteException e) {
                LOG.debug("Encountered a remote exception", e);
                throw new RuntimeException(e);
            }
        }
    }

    after(final LinkedBlockingQueue queue) returning (final FastLeaderElection.Notification notification): pollRecvQueue(queue) {
        this.notification = notification;
        LOG.debug("Received a notification with id = {}", notification.getMessageId());
    }

//    // Intercept state update in the election (within FastLeaderElection)
//
//    pointcut setPeerState(QuorumPeer.ServerState state):
//            within(FastLeaderElection)
//            && call(* QuorumPeer.setPeerState(QuorumPeer.ServerState))
//            && args(state);
//
//    after(final QuorumPeer.ServerState state) returning: setPeerState(state) {
//        final LeaderElectionState leState;
//        switch (state) {
//            case LEADING:
//                leState = LeaderElectionState.LEADING;
//                break;
//            case FOLLOWING:
//                leState = LeaderElectionState.FOLLOWING;
//                break;
//            case OBSERVING:
//                leState = LeaderElectionState.OBSERVING;
//                break;
//            case LOOKING:
//            default:
//                leState = LeaderElectionState.LOOKING;
//                break;
//        }
//        try {
//            LOG.debug("Node {} state: {}", myId, state);
//            testingService.updateLeaderElectionState(myId, leState);
//            if(leState == LeaderElectionState.LOOKING){
//                testingService.updateVote(myId, null);
//            }
//        } catch (final RemoteException e) {
//            LOG.error("Encountered a remote exception", e);
//            throw new RuntimeException(e);
//        }
//    }

    // Intercept state update (within FastLeaderElection && QuorumPeer)

    pointcut setPeerState2(QuorumPeer.ServerState state):
                    call(* org.apache.zookeeper.server.quorum.QuorumPeer.setPeerState(org.apache.zookeeper.server.quorum.QuorumPeer.ServerState))
                    && args(state);

    after(final QuorumPeer.ServerState state) returning: setPeerState2(state) {
        syncFinished = false;
        final LeaderElectionState leState;
        switch (state) {
            case LEADING:
                leState = LeaderElectionState.LEADING;
                break;
            case FOLLOWING:
                leState = LeaderElectionState.FOLLOWING;
                break;
            case OBSERVING:
                leState = LeaderElectionState.OBSERVING;
                break;
            case LOOKING:
            default:
                leState = LeaderElectionState.LOOKING;
                break;
        }
        try {
            LOG.debug("Node {} state: {}", myId, state);
            testingService.updateLeaderElectionState(myId, leState);
            if(leState == LeaderElectionState.LOOKING){
                testingService.updateVote(myId, null);
            }
        } catch (final RemoteException e) {
            LOG.error("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    public String constructPayload(final FastLeaderElection.ToSend toSend) {
        return "from=" + myId +
                ", to=" + toSend.sid +
                ", leader=" + toSend.leader +
                ", state=" + toSend.state +
                ", zxid=0x" + Long.toHexString(toSend.zxid) +
                ", electionEpoch=" + toSend.electionEpoch +
                ", peerEpoch=" + toSend.peerEpoch;
    }

    private org.mpisws.hitmc.api.state.Vote constructVote(final Vote vote) {
        return new org.mpisws.hitmc.api.state.Vote(vote.getId(), vote.getZxid(), vote.getElectionEpoch(), vote.getPeerEpoch());
    }

    // Node state management
    // WorkerReceiver

    public int registerWorkerReceiverSubnode() {
        final int workerReceiverSubnodeId;
        try {
            LOG.debug("Registering WorkerReceiver subnode");
            workerReceiverSubnodeId = testingService.registerSubnode(myId, SubnodeType.WORKER_RECEIVER);
            LOG.debug("Registered WorkerReceiver subnode: id = {}", workerReceiverSubnodeId);
            synchronized (nodeOnlineMonitor) {
                workerReceiverSubnodeRegistered = true;
                if (quorumPeerSubnodeRegistered) {
                    testingService.nodeOnline(myId);
                }
            }
            return workerReceiverSubnodeId;
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    public void deregisterWorkerReceiverSubnode(final int workerReceiverSubnodeId) {
        try {
            LOG.debug("De-registering WorkerReceiver subnode");
            testingService.deregisterSubnode(workerReceiverSubnodeId);
            LOG.debug("De-registered WorkerReceiver subnode");
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

//    public void setWorkerReceiverSending() {
//        synchronized (nodeOnlineMonitor) {
//            workerReceiverSending = true;
//        }
//    }
//
//    public void workerReceiverPostSend(final int subnodeId, final int msgId) throws RemoteException {
//        synchronized (nodeOnlineMonitor) {
//            workerReceiverSending = false;
//            // msgId == -1 means that the sending node is about to be shutdown
//            if (msgId == -1) {
//                // Ensure that other threads are all finished
//                // The last existing subnode is responsible to set the node state as offline
//                if (!quorumPeerSending && !syncProcessorSending) {
//                    testingService.nodeOffline(myId);
//                }
//                awaitShutdown(subnodeId);
//            }
//        }
//    }

    // SyncProcessor

    public int registerSyncProcessorSubnode() {
        final int syncProcessorSubnodeId;
        try {
            LOG.debug("Registering SyncProcessor subnode");
            syncProcessorSubnodeId = testingService.registerSubnode(myId, SubnodeType.SYNC_PROCESSOR);
            LOG.debug("Registered SyncProcessor subnode: id = {}", syncProcessorSubnodeId);
//            synchronized (nodeOnlineMonitor) {
//                syncProcessorSubnodeRegistered = true;
//            }
            return syncProcessorSubnodeId;
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    public void deregisterSyncProcessorSubnode(final int syncProcessorSubnodeId) {
        try {
            LOG.debug("De-registering SyncProcessor subnode");
            testingService.deregisterSubnode(syncProcessorSubnodeId);
            LOG.debug("De-registered SyncProcessor subnode");
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    public String constructRequest(final Request request) {
        return "Node=" + myId +
                ", sessionId=" + request.sessionId +
                ", cxid=" + request.cxid +
                ", type=" + request.type;
    }

//    public void setSyncProcessorSending() {
//        synchronized (nodeOnlineMonitor) {
//            syncProcessorSending = true;
//        }
//    }
//
//    public void syncProcessorPostSend(final int subnodeId, final int msgId) throws RemoteException {
//        synchronized (nodeOnlineMonitor) {
//            syncProcessorSending = false;
//            if (msgId == -1) {
//                // Ensure that other threads are all finished
//                // The last existing subnode is responsible to set the node state as offline
//                if (!quorumPeerSending && !workerReceiverSending) {
//                    testingService.nodeOffline(myId);
//                }
//                awaitShutdown(subnodeId);
//            }
//        }
//    }

    // LearnerHandler

    public void registerLearnerHandlerSubnode(final long threadId, final String threadName){
        try {
            TestingRemoteService testingService = createRmiConnection();
            LOG.debug("Found the remote testing service.");
            LOG.debug("Registering LearnerHandler subnode");
            final int subnodeId = testingService.registerSubnode(myId, SubnodeType.LEARNER_HANDLER);
            LOG.debug("Finish registering LearnerHandler subnode: id = {}", subnodeId);

            SubnodeIntercepter intercepter = new SubnodeIntercepter(threadName, subnodeId, SubnodeType.LEARNER_HANDLER, testingService);
            intercepterMap.put(threadId, intercepter);

            int learnerHandlerSubnodeId = -1;
            try{
                learnerHandlerSubnodeId = intercepterMap.get(threadId).getSubnodeId();
                LOG.debug("this learnerHandlerSubnodeId: {}", learnerHandlerSubnodeId);
            } catch (Exception e) {
                e.printStackTrace();
            }

        } catch (final RemoteException e) {
            LOG.error("Encountered a remote exception.", e);
            throw new RuntimeException(e);
        }
    }


//    public int registerLearnerHandlerSubnode() {
//        final int learnerHandlerSubnodeId;
//        try {
//            LOG.debug("Registering LearnerHandler subnode");
//            learnerHandlerSubnodeId = testingService.registerSubnode(myId, SubnodeType.LEARNER_HANDLER);
//            LOG.debug("Registered LearnerHandler subnode: id = {}", learnerHandlerSubnodeId);
////            synchronized (nodeOnlineMonitor) {
//////                learnerHandlerSubnodeRegistered = true;
//////                learnerHandlerSendingMap.put(learnerHandlerSubnodeId, false);
////            }
//            return learnerHandlerSubnodeId;
//        } catch (final RemoteException e) {
//            LOG.debug("Encountered a remote exception", e);
//            throw new RuntimeException(e);
//        }
//    }

    public void deregisterLearnerHandlerSubnode(final long threadId) {
        try {
            LOG.debug("De-registering LearnerHandler subnode");
            int subnodeId = intercepterMap.get(threadId).getSubnodeId();
            testingService.deregisterSubnode(subnodeId);
            LOG.debug("Finish de-registering LearnerHandler subnode {}", subnodeId);
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    public void addToQueuedPackets(final long threadId, final Object object) {
        final AtomicInteger msgsInQueuedPackets = intercepterMap.get(threadId).getMsgsInQueue();
        msgsInQueuedPackets.incrementAndGet();
        final String payload = constructPacket((QuorumPacket) object);
        LOG.debug("learnerHandlerSubnodeId: {}----------packet: {}", intercepterMap.get(threadId).getSubnodeId(), payload);
        LOG.debug("----------addToQueuedPackets(). msgsInQueuedPackets.size: {}", msgsInQueuedPackets.get());
    }

    public String constructPacket(final QuorumPacket packet) {
        return "learnerHandlerSenderWithinNode=" + myId +
                ", type=" + packet.getType() +
                ", zxid=0x" + Long.toHexString(packet.getZxid()) +
                ", data=" + Arrays.toString(packet.getData()) +
                ", authInfo=" + packet.getAuthinfo();
    }



//    public void setLearnerHandlerSending(final int subnodeId) {
//        synchronized (nodeOnlineMonitor) {
//            learnerHandlerSendingMap.put(subnodeId, true);
//        }
//    }
//
//    public void learnerHandlerPostSend(final int subnodeId, final int msgId) throws RemoteException {
//        synchronized (nodeOnlineMonitor) {
//            learnerHandlerSendingMap.put(subnodeId, false);
//            if (msgId == -1) {
//                // Ensure that other threads are all finished
//                // The last existing subnode is responsible to set the node state as offline
//                if (!quorumPeerSending && !workerReceiverSending) {
//                    testingService.nodeOffline(myId);
//                }
//                awaitShutdown(subnodeId);
//            }
//        }
//    }

//    // Intercept starting the thread
//    // This thread should only be run by the leader
//
//    pointcut runLearnerHandler(): execution(* LearnerHandler.run());
//
//    before(): runLearnerHandler() {
//        LOG.debug("before runLearnerHandler");
//        learnerHandlerSubnodeId = quorumPeerAspect.registerLearnerHandlerSubnode();
//    }
//
//    after(): runLearnerHandler() {
//        LOG.debug("after runLearnerHandler");
//        quorumPeerAspect.deregisterLearnerHandlerSubnode(learnerHandlerSubnodeId);
//    }


    // LearnerHandlerSenderAspect

    public SubnodeIntercepter registerSubnode(final long threadId, final String threadName, final SubnodeType type){
        try {
            TestingRemoteService testingService = createRmiConnection();
            LOG.debug("Found the remote testing service. Registering {} subnode", type);
            final int subnodeId = testingService.registerSubnode(myId, type);
            LOG.debug("Finish registering {} subnode: id = {}", type, subnodeId);
            SubnodeIntercepter intercepter =
                    new SubnodeIntercepter(threadName, subnodeId, type, testingService);
            intercepterMap.put(threadId, intercepter);
            return intercepter;
        } catch (final RemoteException e) {
            LOG.error("Encountered a remote exception.", e);
            throw new RuntimeException(e);
        }
    }

    public void deregisterSubnode(final long threadId) {
        try {
            SubnodeIntercepter intercepter = intercepterMap.get(threadId);
            final SubnodeType type = intercepter.getSubnodeType();
            final int subnodeId = intercepter.getSubnodeId();
            LOG.debug("De-registering {} subnode {}", type, subnodeId);
            testingService.deregisterSubnode(subnodeId);
            LOG.debug("Finish de-registering {} subnode {}", type, subnodeId);
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    // intercept the address of the socket of the learner
    // Identify the ID of this node

    pointcut setMySock(Learner learner):
            call(* Learner.connectToLeader(..)) && target(learner);

    after(final Learner learner): setMySock(learner) {
        try {
            mySock = learner.getSocket();
            LOG.debug("getLocalSocketAddress = {}", mySock.getLocalSocketAddress());
            testingService.registerFollowerSocketInfo(myId, mySock.getLocalSocketAddress().toString());
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }

    //TODO: unregister socket address

//    pointcut newSock(Socket socket):
//            set(Socket Learner.sock) && args(socket);
//
//    after(final Socket sock): newSock(sock) {
//        mySock = sock;
//        LOG.debug("getInetAddress = {}", sock.getInetAddress());
//        LOG.debug("getLocalAddress = {}", sock.getLocalAddress());
//        LOG.debug("getLocalSocketAddress = {}, {}", sock.getLocalSocketAddress(), sock.getLocalPort());
//    }

//    // intercept the initialization of a learner handler for the leader node
//
//    pointcut newLearnerHandler(Socket sock, Leader leader):
//            initialization(LearnerHandler.new(Socket, Leader))
//            && args(sock, leader);
//
//    after(final Socket sock, final Leader leader): newLearnerHandler(sock, leader) {
//        LOG.debug("getLocalSocketAddress = {}", sock.getRemoteSocketAddress());
//    }

    // intercept the effects of network partition
//    pointcut followerProcessPacket


    public String packetToString(QuorumPacket p) {
        String type = null;
        String mess = null;
        Record txn = null;

        switch (p.getType()) {
            case Leader.ACK:
                type = "ACK";
                break;
            case Leader.COMMIT:
                type = "COMMIT";
                break;
            case Leader.FOLLOWERINFO:
                type = "FOLLOWERINFO";
                break;
            case Leader.NEWLEADER:
                type = "NEWLEADER";
                break;
            case Leader.PING:
                type = "PING";
                break;
            case Leader.PROPOSAL:
                type = "PROPOSAL";
                TxnHeader hdr = new TxnHeader();
                try {
                    txn = SerializeUtils.deserializeTxn(p.getData(), hdr);
                    // mess = "transaction: " + txn.toString();
                } catch (IOException e) {
                    LOG.warn("Unexpected exception",e);
                }
                break;
            case Leader.REQUEST:
                type = "REQUEST";
                break;
            case Leader.REVALIDATE:
                type = "REVALIDATE";
                ByteArrayInputStream bis = new ByteArrayInputStream(p.getData());
                DataInputStream dis = new DataInputStream(bis);
                try {
                    long id = dis.readLong();
                    mess = " sessionid = " + id;
                } catch (IOException e) {
                    LOG.warn("Unexpected exception", e);
                }

                break;
            case Leader.UPTODATE:
                type = "UPTODATE";
                break;
            default:
                type = "UNKNOWN" + p.getType();
        }
        String entry = null;
        if (type != null) {
            // TODO: acquire receivign node from remote socket
            entry = type + " " + Long.toHexString(p.getZxid()) + " " + mess
                    + " " + constructPacket(p);
        }
        return entry;
    }





}