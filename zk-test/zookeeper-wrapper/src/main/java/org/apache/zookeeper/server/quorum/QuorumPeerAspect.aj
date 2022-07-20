package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.Request;
import org.disalg.met.api.TestingDef;
import org.disalg.met.api.TestingRemoteService;
import org.disalg.met.api.SubnodeType;
import org.disalg.met.api.state.LeaderElectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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

    private boolean newLeaderDone = false; // for follower

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
    private final Map<Long, SubnodeIntercepter> intercepterMap = new ConcurrentHashMap();

    /***
     * THis structure is for subnodes that are multiple of one type in a node.
     */
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

    public Map<Long, SubnodeIntercepter> getIntercepterMap() {
        return intercepterMap;
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

    // For follower QuorumPeer thread
    public void setSyncFinished(boolean syncFinished) {
        this.syncFinished = syncFinished;
    }
    // For follower QuorumPeer thread
    public boolean isSyncFinished() {
        return syncFinished;
    }
    // For follower QuorumPeer thread
    public void setNewLeaderDone(boolean newLeaderDone) {
        this.newLeaderDone = newLeaderDone;
    }
    // For follower QuorumPeer thread
    public boolean isNewLeaderDone() {
        return newLeaderDone;
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

    // TODO: change this to boolean around()
    boolean around(final Object object): offerWithinFastLeaderElection(object) {
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
            lastSentMessageId = testingService.offerElectionMessage(quorumPeerSubnodeId, (int) toSend.sid, predecessorIds, payload);
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

//            // TODO: to check if the partition happens with around()
            if (lastSentMessageId == TestingDef.RetCode.NODE_PAIR_IN_PARTITION){
                // just drop the message
                LOG.debug("partition occurs! just drop the message. What about other types of messages?");
                testingService.setReceivingState(quorumPeerSubnodeId);
                // confirm the return value
                return false;
            }
            return proceed(object);
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        } catch (final Exception e) {
            LOG.debug("Uncaught exception when intercepting", e);
            throw new RuntimeException(e); // new added
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
        synchronized (nodeOnlineMonitor) {
            sendingSubnodeNum.incrementAndGet();
            LOG.debug("add sendingSubnodeNum: {}", sendingSubnodeNum.get());
        }
    }

    public void postSend(final int subnodeId, final int msgId) throws RemoteException {
        synchronized (nodeOnlineMonitor) {
            final int existingSendingSubnodeNum = sendingSubnodeNum.decrementAndGet();
            LOG.debug("decrease sendingSubnodeNum: {}", sendingSubnodeNum.get());
            if (msgId == TestingDef.RetCode.NODE_CRASH) {
                // The last existing subnode is responsible to set the node state as offline
                LOG.debug("-----subnodeId: {}, msgId: {}, existingSendingSubnodeNum: {}", subnodeId, msgId, existingSendingSubnodeNum);
                if (existingSendingSubnodeNum == 0) {
                    LOG.debug("-----going to set nodeOffline by subnodeId: {}, msgId: {}", subnodeId, msgId);
                    testingService.nodeOffline(myId);
                }
                awaitShutdown(subnodeId);
            }
        }
    }

    public void awaitShutdown(final int subnodeId) {
        try {
            LOG.debug("awaitShutdown. to deregister subnode {}", subnodeId);
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
                syncFinished = false;
                newLeaderDone = false;
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

    private org.disalg.met.api.state.Vote constructVote(final Vote vote) {
        return new org.disalg.met.api.state.Vote(vote.getId(), vote.getZxid(), vote.getElectionEpoch(), vote.getPeerEpoch());
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

    public int registerSubnode(final TestingRemoteService testingService, final SubnodeType subnodeType) {
        try {
            LOG.debug("Found the remote testing service. Registering {} subnode", subnodeType);
            final int subnodeId = testingService.registerSubnode(myId, subnodeType);
            LOG.debug("Finish registering {} subnode: id = {}", subnodeType, subnodeId);
            return subnodeId;
        } catch (final RemoteException e) {
            LOG.error("Encountered a remote exception.", e);
            throw new RuntimeException(e);
        }
    }

    public void deregisterSubnode(final TestingRemoteService testingService, final int subnodeId, final SubnodeType subnodeType) {
        try {
            LOG.debug("De-registering {} subnode {}", subnodeType, subnodeId);
            testingService.deregisterSubnode(subnodeId);
            LOG.debug("Finish de-registering {} subnode {}", subnodeType, subnodeId);
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }
    }


    /***
     * The following registerSubnode() and deregisterSubnode() methods are for threads that will be run more than one
     * in a node such as LearnerHandler and LearnerHandlerSender
     * These subnode info will be stored using the SubnodeInterceptor structure
     * @param threadId
     * @param threadName
     * @param subnodeType
     * @return
     */

    public SubnodeIntercepter registerSubnode(final long threadId, final String threadName, final SubnodeType subnodeType){
        try {
            TestingRemoteService testingService = createRmiConnection();
            LOG.debug("Found the remote testing service. Registering {} subnode", subnodeType);
            final int subnodeId = testingService.registerSubnode(myId, subnodeType);
            LOG.debug("Finish registering {} subnode: id = {}", subnodeType, subnodeId);
            SubnodeIntercepter intercepter =
                    new SubnodeIntercepter(threadName, subnodeId, subnodeType, testingService);
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
            final SubnodeType subnodeType = intercepter.getSubnodeType();
            final int subnodeId = intercepter.getSubnodeId();
            LOG.debug("De-registering {} subnode {}", subnodeType, subnodeId);
            testingService.deregisterSubnode(subnodeId);
            LOG.debug("Finish de-registering {} subnode {}", subnodeType, subnodeId);
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

    public void addToQueuedPackets(final long threadId, final Object object) {
        final AtomicInteger msgsInQueuedPackets = intercepterMap.get(threadId).getMsgsInQueue();
        msgsInQueuedPackets.incrementAndGet();
        final String payload = packetToString((QuorumPacket) object);
        LOG.debug("learnerHandlerSubnodeId: {}----------packet: {}", intercepterMap.get(threadId).getSubnodeId(), payload);
        LOG.debug("----------addToQueuedPackets(). msgsInQueuedPackets.size: {}", msgsInQueuedPackets.get());
    }

    public String packetToString(QuorumPacket p) {
        String type = null;
//        String mess = null;
//        Record txn = null;

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
//                TxnHeader hdr = new TxnHeader();
//                try {
//                    txn = SerializeUtils.deserializeTxn(p.getData(), hdr);
//                    // mess = "transaction: " + txn.toString();
//                } catch (IOException e) {
//                    LOG.warn("Unexpected exception",e);
//                }
                break;
            case Leader.REQUEST:
                type = "REQUEST";
                break;
            case Leader.REVALIDATE:
                type = "REVALIDATE";
//                ByteArrayInputStream bis = new ByteArrayInputStream(p.getData());
//                DataInputStream dis = new DataInputStream(bis);
//                try {
//                    long id = dis.readLong();
//                    mess = " sessionid = " + id;
//                } catch (IOException e) {
//                    LOG.warn("Unexpected exception", e);
//                }

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
            entry = "type=" + type +
                    ", typeId=" + p.getType() +
                    ", sendingNode=" + myId +
                    ", zxid=0x" + Long.toHexString(p.getZxid());
        }
        return entry;
    }

    public String constructRequest(final Request request) {
        return "Node=" + myId +
                ", sessionId=" + request.sessionId +
                ", cxid=" + request.cxid +
                ", zxid=0x" + Long.toHexString(request.zxid) +
                ", typeId=" + request.type;
    }





}