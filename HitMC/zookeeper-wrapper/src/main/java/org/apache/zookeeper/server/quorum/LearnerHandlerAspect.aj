package org.apache.zookeeper.server.quorum;

import org.apache.jute.Record;
import org.mpisws.hitmc.api.SubnodeType;
import org.mpisws.hitmc.api.TestingDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/***
 * This intercepts the message sending process of the learnerHandler threads on the leader side
 */
public aspect LearnerHandlerAspect {

    private static final Logger LOG = LoggerFactory.getLogger(LearnerHandlerAspect.class);

    private final QuorumPeerAspect quorumPeerAspect = QuorumPeerAspect.aspectOf();

//    private final TestingRemoteService testingService;

//    private int learnerHandlerSubnodeId;

//    private Integer lastPacketId = null;

//    private final AtomicInteger msgsInQueuedPackets = new AtomicInteger(0);

//    // A lock
//    private final Object learnerHandlerOnlineMonitor = new Object();

//    public AtomicInteger getMsgsInQueuedPackets() {
//        return msgsInQueuedPackets;
//    }

////     TODO: This is a singleton. Is it OK?
//    public LearnerHandlerAspect() {
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

//    public TestingRemoteService getTestingService() {
//        return testingService;
//    }

    // Intercept starting the thread
    // This thread should only be run by the leader

    pointcut runLearnerHandler(): execution(* org.apache.zookeeper.server.quorum.LearnerHandler.run());

    before(): runLearnerHandler() {
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("before runLearnerHandler-------Thread: {}, {}------", threadId, threadName);
        quorumPeerAspect.registerSubnode(
                Thread.currentThread().getId(), Thread.currentThread().getName(), SubnodeType.LEARNER_HANDLER);
        // Set RECEIVING state since there is nowhere else to set
    }

    after(): runLearnerHandler() {
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("after runLearnerHandler-------Thread: {}, {}------", threadId, threadName);
        quorumPeerAspect.deregisterSubnode(Thread.currentThread().getId());
    }


    // intercept the sender thread created by a learner handler

    pointcut runLearnerHandlerSender(java.lang.Thread childThread):
            withincode(* org.apache.zookeeper.server.quorum.LearnerHandler.run())
                && call(* java.lang.Thread.start())
                && target(childThread);

    before(java.lang.Thread childThread): runLearnerHandlerSender(childThread) {
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        final long childThreadId = childThread.getId();
        final String childThreadName = childThread.getName();
        LOG.debug("before runSender-------parent thread {}: {}------", threadId, threadName);
        LOG.debug("before runSender-------child Thread {}: {}------", childThreadId, childThreadName);
        // TODO: record its parent thread
        quorumPeerAspect.registerSubnode(childThreadId, childThreadName, SubnodeType.LEARNER_HANDLER_SENDER);
    }

//    after(java.lang.Thread childThread): runLearnerHandlerSender(childThread) {
//        final long threadId = Thread.currentThread().getId();
//        final String threadName = Thread.currentThread().getName();
//        final long childThreadId = childThread.getId();
//        final String childThreadName = childThread.getName();
//        LOG.debug("after runSender-------parent thread: {}, {}------", threadId, threadName);
//        LOG.debug("after runSender-------child Thread: {}, {}------", childThreadId, childThreadName);
//        quorumPeerAspect.deregisterSubnode(childThreadId);
//    }

    // TODO: find a better way to intercept
    pointcut closeSock():
            within(org.apache.zookeeper.server.quorum.LearnerHandler) && call(* java.net.Socket.close());

    after(): closeSock() {
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("after closeSock-------Thread: {}, {}------", threadId, threadName);
        quorumPeerAspect.deregisterSubnode(threadId);
    }




    // Intercept message offering within LearnerHandlerSenderAspect

//    pointcut pollFromQueue(LinkedBlockingQueue queue):
//            within(LearnerHandler)
//                    && call(* LinkedBlockingQueue.poll())
//                    && target(queue);
//
//    Object around(LinkedBlockingQueue queue): pollFromQueue(queue) {
//        LOG.debug("------around-before");
//        final long threadId = Thread.currentThread().getId();
//        final String threadName = Thread.currentThread().getName();
//        LOG.debug("before advice of learner handler send-------Thread: {}, {}------", threadId, threadName);
//
//        QuorumPeerAspect.SubnodeIntercepter intercepter = quorumPeerAspect.getIntercepter(threadId);
//        int subnodeId;
//        try{
//            subnodeId = intercepter.getSubnodeId();
//        } catch (RuntimeException e) {
//            LOG.debug("--------catch exception: {}", e.toString());
//            throw new RuntimeException(e);
//        }
//        final AtomicInteger msgsInQueuedPackets = intercepter.getMsgsInQueue();
//        LOG.debug("--------------My queuedPackets has {} element. msgsInQueuedPackets has {}.",
//                queue.size(), msgsInQueuedPackets.get());
//
//        // to check if the sending node and the partition
//
////        if (msgsInQueuedRequests.get() == 0) {
//        if (queue.isEmpty()) {
//            // Going to block here. Better notify the scheduler
//            LOG.debug("--------------poll empty! My queuedPackets has {} element. Set subnode {} to RECEIVING state." +
//                    " Will be blocked until some packet enqueues", queue.size(), subnodeId);
//            try {
//                intercepter.getTestingService().setReceivingState(subnodeId);
//            } catch (final RemoteException e) {
//                LOG.debug("Encountered a remote exception", e);
//                throw new RuntimeException(e);
//            }
//        }
//
//        Object packet = proceed(queue);
//
//        LOG.debug("------around-after: {}", packet);
//        LOG.debug("after advice of learner handler send-------Thread: {}, {}------", threadId, threadName);
//        final int toBeSentNum = msgsInQueuedPackets.get();
//        LOG.debug("--------------My queuedPackets has {} element left after takeOrPollFromQueuedPackets. " +
//                "msgsInQueuedPackets has {}", queue.size(), toBeSentNum);
//        if (packet == null){
//            LOG.debug("------It's null! Will go to flush and use take().");
//            return packet;
//        }
//        if (packet instanceof QuorumPacket) {
//            LOG.debug("It's a packet!");
////            final String payload = quorumPeerAspect.constructPacket((QuorumPacket) packet);
//            final int type =  ((QuorumPacket) packet).getType();
//
//            final String payload = packetToString((QuorumPacket) packet);
//            LOG.debug("---------Taking the packet ({}) from queued packets. Won't intercept.", payload);
//
//            if (type != Leader.PROPOSAL){
//                return packet;
//            }
//
//            try {
//                // before offerMessage: increase sendingSubnodeNum
//                quorumPeerAspect.setSubnodeSending();
//                final String receivingAddr = threadName.split("-")[1];
//                final int lastPacketId = intercepter.getTestingService().offerMessageToFollower(subnodeId, receivingAddr, payload, type);
//                LOG.debug("lastPacketId = {}", lastPacketId);
//                // to check if the node is crashed
//                // after offerMessage: decrease sendingSubnodeNum and shutdown this node if sendingSubnodeNum == 0
//                quorumPeerAspect.postSend(subnodeId, lastPacketId);
//                // to check if the client initialization is done
//                if (lastPacketId == TestingDef.RetCode.CLIENT_INITIALIZATION_NOT_DONE){
//                    LOG.debug("----client initialization is not done!---");
//                    return packet;
//                }
//                // to check if the partition happens
////                if (lastPacketId == TestingDef.RetCode.NODE_PAIR_IN_PARTITION){
////                    // just drop the message
////                    packet = null;
////                    return packet;
////                }
//            } catch (RemoteException e) {
//                e.printStackTrace();
//            }
//////            msgsInQueuedPackets.decrementAndGet();
//        }
//        return packet;
//    }

//    pointcut takeFromQueue(LinkedBlockingQueue queue):
//            within(LearnerHandler)
//                    && call(* LinkedBlockingQueue.take())
//                    && target(queue);
//
//    Object around(LinkedBlockingQueue queue): takeFromQueue(queue) {
//        LOG.debug("------take around-before");
//        final long threadId = Thread.currentThread().getId();
//        final String threadName = Thread.currentThread().getName();
//        LOG.debug("before take advice of learner handler send-------Thread: {}, {}------", threadId, threadName);
//
//        QuorumPeerAspect.SubnodeIntercepter intercepter = quorumPeerAspect.getIntercepter(threadId);
//        int subnodeId;
//        try{
//            subnodeId = intercepter.getSubnodeId();
//        } catch (RuntimeException e) {
//            LOG.debug("--------catch exception: {}", e.toString());
//            throw new RuntimeException(e);
//        }
//        final AtomicInteger msgsInQueuedPackets = intercepter.getMsgsInQueue();
//        LOG.debug("--------------My queuedPackets has {} element. msgsInQueuedPackets has {}.",
//                queue.size(), msgsInQueuedPackets.get());
//
//        // to check if the sending node and the partition
//
//
////        if (msgsInQueuedRequests.get() == 0) {
//        if (queue.isEmpty()) {
//            // Going to block here. Better notify the scheduler
//            LOG.debug("--------------take empty! My queuedPackets has {} element. Set subnode {} to RECEIVING state." +
//                    " Will be blocked until some packet enqueues", queue.size(), subnodeId);
//            try {
//                intercepter.getTestingService().setReceivingState(subnodeId);
//            } catch (final RemoteException e) {
//                LOG.debug("Encountered a remote exception", e);
//                throw new RuntimeException(e);
//            }
//        }
//
//        Object packet = proceed(queue);
//
//        LOG.debug("------around-after: {}", packet);
//        LOG.debug("after advice of learner handler send-------Thread: {}, {}------", threadId, threadName);
//        final int toBeSentNum = msgsInQueuedPackets.get();
//        LOG.debug("--------------My queuedPackets has {} element left after takeOrPollFromQueuedPackets. " +
//                "msgsInQueuedPackets has {}", queue.size(), toBeSentNum);
//        if (packet == null){
//            LOG.debug("------It's null! Will go to flush and use take().");
//            return packet;
//        }
//        if (packet instanceof QuorumPacket) {
//            LOG.debug("It's a packet!");
////            final String payload = quorumPeerAspect.constructPacket((QuorumPacket) packet);
//            final int type =  ((QuorumPacket) packet).getType();
//
//            final String payload = packetToString((QuorumPacket) packet);
//            LOG.debug("---------Taking the packet ({}) from queued packets. Won't intercept.", payload);
//
//            if (type != Leader.PROPOSAL){
//                return packet;
//            }
//
//            try {
//                // before offerMessage: increase sendingSubnodeNum
//                quorumPeerAspect.setSubnodeSending();
//                final String receivingAddr = threadName.split("-")[1];
//                final int lastPacketId = intercepter.getTestingService().offerMessageToFollower(subnodeId, receivingAddr, payload, type);
//                LOG.debug("lastPacketId = {}", lastPacketId);
//                // to check if the node is crashed
//                // after offerMessage: decrease sendingSubnodeNum and shutdown this node if sendingSubnodeNum == 0
//                quorumPeerAspect.postSend(subnodeId, lastPacketId);
//                // to check if the client initialization is done
//                if (lastPacketId == TestingDef.RetCode.CLIENT_INITIALIZATION_NOT_DONE){
//                    LOG.debug("----client initialization is not done!---");
//                    return packet;
//                }
//                // to check if the partition happens
////                if (lastPacketId == TestingDef.RetCode.NODE_PAIR_IN_PARTITION){
////                    // just drop the message
////                    packet = null;
////                    return packet;
////                }
//            } catch (RemoteException e) {
//                e.printStackTrace();
//            }
//////            msgsInQueuedPackets.decrementAndGet();
//        }
//        return packet;
//    }

    /***
     * For LearnerHandlerSender
     * Set RECEIVING state when the queue is empty
     */
    pointcut takeOrPollFromQueue(LinkedBlockingQueue queue):
            within(org.apache.zookeeper.server.quorum.LearnerHandler)
                    && (call(* LinkedBlockingQueue.poll()) || call(* LinkedBlockingQueue.take()))
                    && target(queue);

    before(final LinkedBlockingQueue queue): takeOrPollFromQueue(queue) {
        // TODO: Aspect of aspect
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("before advice of learner handler send-------Thread: {}, {}------", threadId, threadName);

        QuorumPeerAspect.SubnodeIntercepter intercepter = quorumPeerAspect.getIntercepter(threadId);
        int subnodeId;
        try{
            subnodeId = intercepter.getSubnodeId();
        } catch (RuntimeException e) {
            LOG.debug("--------catch exception: {}", e.toString());
            throw new RuntimeException(e);
        }
        final AtomicInteger msgsInQueuedPackets = intercepter.getMsgsInQueue();
        LOG.debug("--------------My queuedPackets has {} element. msgsInQueuedPackets has {}.",
                queue.size(), msgsInQueuedPackets.get());
//        if (msgsInQueuedRequests.get() == 0) {
        if (queue.isEmpty()) {
            // Going to block here. Better notify the scheduler
            LOG.debug("--------------Checked empty! My queuedPackets has {} element. Set subnode {} to RECEIVING state." +
                    " Will be blocked until some packet enqueues", queue.size(), subnodeId);
            try {
                intercepter.getTestingService().setReceivingState(subnodeId);
            } catch (final RemoteException e) {
                LOG.debug("Encountered a remote exception", e);
                throw new RuntimeException(e);
            }
        }
    }

//    after(final LinkedBlockingQueue queue) returning (Object packet): takeOrPollFromQueue(queue) {
//        final long threadId = Thread.currentThread().getId();
//        final String threadName = Thread.currentThread().getName();
//        LOG.debug("after advice of learner handler send-------Thread: {}, {}------", threadId, threadName);
//
//        QuorumPeerAspect.SubnodeIntercepter intercepter = quorumPeerAspect.getIntercepter(threadId);
//        final AtomicInteger msgsInQueuedPackets = intercepter.getMsgsInQueue();
//        final int toBeSentNum = msgsInQueuedPackets.get();
//        LOG.debug("--------------My queuedPackets has {} element left after takeOrPollFromQueuedPackets. " +
//                "msgsInQueuedPackets has {}", queue.size(), toBeSentNum);
//        if (packet == null){
//            LOG.debug("------It's null! Will go to flush and use take().");
//            return;
//        }
//        if (packet instanceof QuorumPacket) {
//            LOG.debug("It's a packet!");
////            final String payload = quorumPeerAspect.constructPacket((QuorumPacket) packet);
//            final int type =  ((QuorumPacket) packet).getType();
////            switch (type) {
////                case Leader.REQUEST:
//////                case Leader.PROPOSAL:
////                case Leader.ACK:
////                case Leader.COMMIT:
////                case Leader.PING:        // PING type is also here
////                case Leader.REVALIDATE:
////                case Leader.SYNC:
////                case Leader.INFORM:
////                case Leader.NEWLEADER:
////                case Leader.FOLLOWERINFO:
////                case Leader.UPTODATE:
////                case Leader.DIFF:
////                case Leader.TRUNC:
////                case Leader.SNAP:
////                case Leader.OBSERVERINFO:
////                case Leader.LEADERINFO:
////                case Leader.ACKEPOCH:
////                    LOG.debug("---------Taking the packet ({}) from queued packets. Won't intercept.", payload);
//////                    msgsInQueuedPackets.decrementAndGet();
////                    return;
////                default:
////            };
//
//            final String payload = packetToString((QuorumPacket) packet);
//            LOG.debug("---------Taking the packet ({}) from queued packets. Won't intercept.", payload);
//
//            if (type != Leader.PROPOSAL){
//                return;
//            }
//
//            // TODO: intercept the proposal message sending process
//
//            try {
//                int subnodeId;
//                try{
//                    subnodeId = intercepter.getSubnodeId();
//                } catch (RuntimeException e) {
//                    LOG.debug("--------catch exception when acquiring subnode id: {}", e.toString());
//                    throw new RuntimeException(e);
//                }
//
//                // before offerMessage: increase sendingSubnodeNum
//                quorumPeerAspect.setSubnodeSending();
//                // TODO: how to get receivingNodeId?
//                final String receivingAddr = threadName.split("-")[1];
//                final int lastPacketId = intercepter.getTestingService().offerMessageToFollower(subnodeId, receivingAddr, payload, type);
//                LOG.debug("lastPacketId = {}", lastPacketId);
//                // to check if the node is crashed
//                // after offerMessage: decrease sendingSubnodeNum and shutdown this node if sendingSubnodeNum == 0
//                quorumPeerAspect.postSend(subnodeId, lastPacketId);
//                // to check if the partition happens
//                if (lastPacketId == TestingDef.RetCode.NODE_PAIR_IN_PARTITION){
//                    // just drop the message
//                    packet = null;
//                    return;
//                }
//            } catch (RemoteException e) {
//                e.printStackTrace();
//            }
//////            msgsInQueuedPackets.decrementAndGet();
//        }
//    }

//    // TODO: catch args, check request type
//    // Attention: the add() method is from the class AbstractQueue
//    pointcut addToQueuedPackets(Object object):
//            within(LearnerHandler)
//            && call(* java.util.AbstractQueue.add(java.lang.Object))
//            && if (object instanceof QuorumPacket)
//            && args(object);
//
//
//    after(final Object object) returning: addToQueuedPackets(object) {
//        final long threadId = Thread.currentThread().getId();
//        final String threadName = Thread.currentThread().getName();
//        LOG.debug("after addToQueuedPackets-------Thread: {}, {}------", threadId, threadName);
//        quorumPeerAspect.addToQueuedPackets(threadId, object);
//
////        msgsInQueuedPackets.incrementAndGet();
////        final String payload = quorumPeerAspect.constructPacket((QuorumPacket) object);
////        LOG.debug("learnerHandlerSubnodeId: {}----------packet: {}", learnerHandlerSubnodeId, payload);
////        LOG.debug("----------addToQueuedPackets(). msgsInQueuedPackets.size: {}", msgsInQueuedPackets.get());
//    }

    /***
     * For LearnerHandlerSender sending messages to followers during SYNC & BROADCAST phase
     *  --> LeaderSyncFollower: send NEWLEADER
     *  --> LeaderProcessACKLD: send UPTODATE
     * For BROADCAST phase
     *  --> LeaderProcessRequest: send PROPOSAL to quorum followers
     *  --> LeaderProcessACK : send COMMIT after receiving quorum's logRequest (PROPOSAL) ACKs
     */
    pointcut writeRecord(Record r, String s):
            within(org.apache.zookeeper.server.quorum.LearnerHandler) && !withincode(void java.lang.Runnable.run()) &&
            call(* org.apache.jute.BinaryOutputArchive.writeRecord(Record, String)) && args(r, s);

    void around(Record r, String s): writeRecord(r, s) {
        LOG.debug("------around-before writeRecord");
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("before advice of learner handler send-------Thread: {}, {}------", threadId, threadName);

        QuorumPeerAspect.SubnodeIntercepter intercepter = quorumPeerAspect.getIntercepter(threadId);
        int subnodeId;
        try{
            subnodeId = intercepter.getSubnodeId();
        } catch (RuntimeException e) {
            LOG.debug("--------catch exception: {}", e.toString());
            throw new RuntimeException(e);
        }

        QuorumPacket packet = (QuorumPacket) r;
        final String payload = quorumPeerAspect.packetToString(packet);
        final int type =  packet.getType();
        switch (type) {
            case Leader.NEWLEADER:
                LOG.debug("-------sending UPTODATE!!!!-------begin to serve clients");
                break;
            case Leader.UPTODATE:
                intercepter.setSyncFinished(true);
                LOG.debug("-------sending UPTODATE!!!!-------begin to serve clients");
                break;
            case Leader.COMMIT:
                LOG.debug("-------sending COMMIT!!!!");
                break;
            case Leader.PROPOSAL:
                LOG.debug("-------sending PROPOSAL!!!!");
                break;
            case Leader.DIFF:
            case Leader.TRUNC:
            case Leader.SNAP:
            case Leader.REQUEST:
            case Leader.ACK:
            case Leader.PING:
            case Leader.REVALIDATE:
            case Leader.SYNC:
            case Leader.INFORM:
            case Leader.FOLLOWERINFO:
            case Leader.OBSERVERINFO:
            case Leader.LEADERINFO:
            case Leader.ACKEPOCH:
                LOG.debug("---------Taking the packet ({}) from queued packets. Won't intercept. Subnode: {}",
                        payload, subnodeId);
                proceed(r, s);
                return;
        }
//        if (!intercepter.isSyncFinished() || (type != Leader.PROPOSAL && type != Leader.COMMIT)){
//            proceed(r, s);
//            return;
//        }

//        if (!intercepter.isSyncFinished() ||
//                (type != Leader.PROPOSAL && type != Leader.COMMIT && type != Leader.UPTODATE && type != Leader.NEWLEADER)){
//            proceed(r, s);
//            return;
//        }


        try {
            // before offerMessage: increase sendingSubnodeNum
            quorumPeerAspect.setSubnodeSending();

            final String receivingAddr = threadName.split("-")[1];
            final long zxid = packet.getZxid();
            final int lastPacketId = intercepter.getTestingService()
                    .offerLeaderToFollowerMessage(subnodeId, receivingAddr, zxid, payload, type);
            LOG.debug("lastPacketId = {}", lastPacketId);

            // to check if the node is crashed
            // after offerMessage: decrease sendingSubnodeNum and shutdown this node if sendingSubnodeNum == 0
            quorumPeerAspect.postSend(subnodeId, lastPacketId);

            // TODO: confirm this check before partition check is ok by checking the code of LearnerHandler
            if (type == Leader.UPTODATE) {
                quorumPeerAspect.getTestingService().readyForBroadcast(subnodeId);
            }

            // Trick: set RECEIVING state here
            intercepter.getTestingService().setReceivingState(subnodeId);

            // to check if the partition happens
            if (lastPacketId == TestingDef.RetCode.NODE_PAIR_IN_PARTITION){
                // just drop the message
                LOG.debug("partition occurs! just drop the message. What about other types of messages?");
                return;
            }

            proceed(r, s);
        } catch (RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }

    }

    /***
     * For LearnerHandler sending followers' message during SYNC phase immediately without adding to the queue
     * package type:
     * (for ZAB1.0) LEADERINFO (17)
     * (for ZAB < 1.0) NEWLEADER (10)
     * (for ZAB1.0) DIFF (13) / TRUNC (14) / SNAP (15)
     *  --> since LeaderSyncFollower: QuorumPacket.getType() == NEWLEADER (10)
     *  --> For now we pass DIFF (13) / TRUNC (14) / SNAP (15) so this pointcut is deprecated
     */
    @Deprecated
    pointcut learnerHandlerWriteRecord(Record r, String s):
            withincode(* org.apache.zookeeper.server.quorum.LearnerHandler.run()) &&
                    call(* org.apache.jute.BinaryOutputArchive.writeRecord(Record, String)) && args(r, s);

    void around(Record r, String s): learnerHandlerWriteRecord(r, s) {
        LOG.debug("------around-before learnerHandlerWriteRecord");
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("before advice of learner handler-------Thread: {}, {}------", threadId, threadName);

        QuorumPeerAspect.SubnodeIntercepter intercepter = quorumPeerAspect.getIntercepter(threadId);
        int subnodeId = -1;
        try{
            subnodeId = intercepter.getSubnodeId();
        } catch (RuntimeException e) {
            LOG.debug("--------catch exception: {}", e.toString());
            throw new RuntimeException(e);
        }

        // Intercept QuorumPacket
        QuorumPacket packet = (QuorumPacket) r;
        final String payload = quorumPeerAspect.packetToString(packet);


        final int type =  packet.getType();
        LOG.debug("--------------I am a LearnerHandler. QuorumPacket {}. Set subnode {} to RECEIVING state. Type: {}",
                payload, subnodeId, type);
        // Set RECEIVING state since there is nowhere else to set
        try {
            intercepter.getTestingService().setReceivingState(subnodeId);
        } catch (final RemoteException e) {
            LOG.debug("Encountered a remote exception", e);
            throw new RuntimeException(e);
        }

        proceed(r, s);
        return;


//        if (type != Leader.DIFF && type != Leader.TRUNC && type != Leader.SNAP){
//            LOG.info("------------Type will always be LEADERINFO / DIFF / TRUNC / SNAP!---------");
//            proceed(r, s);
//            return;
//        }
//
////        if (type != Leader.PROPOSAL){
////            LOG.info("------------Type will always be LEADERINFO / DIFF / TRUNC / SNAP!---------");
////            proceed(r, s);
////            return;
////        }
//
//        try {
//            // before offerMessage: increase sendingSubnodeNum
//            quorumPeerAspect.setSubnodeSending();
//
//            final String receivingAddr = threadName.split("-")[1];
//            final long zxid = packet.getZxid();
//            final int lastPacketId = intercepter.getTestingService()
//                    .offerLeaderToFollowerMessage(subnodeId, receivingAddr, zxid, payload, type);
////            final int lastPacketId = intercepter.getTestingService()
////                    .offerInternalMessageToFollower(subnodeId, receivingAddr, zxid, payload, type);
//            LOG.debug("lastPacketId = {}", lastPacketId);
//
//            // to check if the node is crashed
//            // after offerMessage: decrease sendingSubnodeNum and shutdown this node if sendingSubnodeNum == 0
//            quorumPeerAspect.postSend(subnodeId, lastPacketId);
//
//            // Trick: set RECEIVING state here
//            intercepter.getTestingService().setReceivingState(subnodeId);
//
//            // to check if the partition happens
//            if (lastPacketId == TestingDef.RetCode.NODE_PAIR_IN_PARTITION){
//                // just drop the message
//                LOG.debug("partition occurs! just drop the message. What about other types of messages?");
//                return;
//            }
//            proceed(r, s);
//
//        } catch (RemoteException e) {
//            LOG.debug("Encountered a remote exception", e);
//            throw new RuntimeException(e);
//        }
    }
}
