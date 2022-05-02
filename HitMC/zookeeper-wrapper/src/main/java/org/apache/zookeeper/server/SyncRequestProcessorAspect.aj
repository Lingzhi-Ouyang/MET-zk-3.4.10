package org.apache.zookeeper.server;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.server.quorum.QuorumPeerAspect;
import org.mpisws.hitmc.api.SubnodeType;
import org.mpisws.hitmc.api.TestingRemoteService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public aspect SyncRequestProcessorAspect {

    private static final Logger LOG = LoggerFactory.getLogger(SyncRequestProcessorAspect.class);

    private final QuorumPeerAspect quorumPeerAspect = QuorumPeerAspect.aspectOf();

//    private final TestingRemoteService testingService;
//
//    private int syncProcessorSubnodeId;
//
//    private Integer lastSyncRequestId = null;
//
////    // Keep track of the request message
////    private Request request;
////
////    public Request getRequest() {
////        return request;
////    }
//
//    private final AtomicInteger msgsInQueuedRequests = new AtomicInteger(0);
//
//    public AtomicInteger getMsgsInQueuedRequests() {
//        return msgsInQueuedRequests;
//    }
//
//    public SyncRequestProcessorAspect() {
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
//    public TestingRemoteService getTestingService() {
//        return testingService;
//    }

//    // Intercept starting the SyncRequestProcessor thread
//
//    pointcut runSyncProcessor(): execution(* SyncRequestProcessor.run());
//
//    before(): runSyncProcessor() {
//        LOG.debug("-------Thread: {}------", Thread.currentThread().getName());
//        LOG.debug("before runSyncProcessor");
//        syncProcessorSubnodeId = quorumPeerAspect.registerSyncProcessorSubnode();
//    }
//
//    after(): runSyncProcessor() {
//        LOG.debug("after runSyncProcessor");
//        quorumPeerAspect.deregisterSyncProcessorSubnode(syncProcessorSubnodeId);
//    }

    // Intercept starting the SyncRequestProcessor thread

    pointcut runSyncProcessor(): execution(* SyncRequestProcessor.run());

    before(): runSyncProcessor() {
        LOG.debug("-------Thread {}: {}------",Thread.currentThread().getId(), Thread.currentThread().getName());
        LOG.debug("before runSyncProcessor");
        quorumPeerAspect.registerSubnode(
                Thread.currentThread().getId(), Thread.currentThread().getName(), SubnodeType.SYNC_PROCESSOR);
    }

    after(): runSyncProcessor() {
        LOG.debug("after runSyncProcessor");
        quorumPeerAspect.deregisterSubnode(Thread.currentThread().getId());
    }

    // Intercept message processed within SyncRequestProcessor
    // candidate 1: processRequest() called by its previous processor
    // Use candidate 2: LinkedBlockingQueue.take() / poll()

    // Intercept polling the receive queue of the queuedRequests within SyncRequestProcessor

    pointcut takeOrPollFromQueue(LinkedBlockingQueue queue):
            within(SyncRequestProcessor)
                    && (call(* LinkedBlockingQueue.take())
                    || call(* LinkedBlockingQueue.poll()))
                    && target(queue);

    before(final LinkedBlockingQueue queue): takeOrPollFromQueue(queue) {
        // TODO: Aspect of aspect
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("before advice of sync-------Thread: {}, {}------", threadId, threadName);

        QuorumPeerAspect.SubnodeIntercepter intercepter = quorumPeerAspect.getIntercepter(threadId);
        int subnodeId;
        try{
            subnodeId = intercepter.getSubnodeId();
        } catch (RuntimeException e) {
            LOG.debug("--------catch exception: {}", e.toString());
            throw new RuntimeException(e);
        }
        LOG.debug("--------------My queuedRequests has {} element. msgsInQueuedRequests has {}.",
                queue.size(), intercepter.getMsgsInQueue().get());
//        if (msgsInQueuedRequests.get() == 0) {
        if (queue.isEmpty()) {
            // Going to block here. Better notify the scheduler
            LOG.debug("--------------Checked! My toSync queuedRequests has {} element. Go to RECEIVING state." +
                    " Will be blocked until some request enqueues when nothing to flush", queue.size());
            try {
                intercepter.getTestingService().setReceivingState(subnodeId);
            } catch (final RemoteException e) {
                LOG.debug("Encountered a remote exception", e);
                throw new RuntimeException(e);
            }
        }
    }

    after(final LinkedBlockingQueue queue) returning (final Object request): takeOrPollFromQueue(queue) {
        // TODO: Aspect of aspect
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("after advice of sync-------Thread: {}, {}------", threadId, threadName);

        QuorumPeerAspect.SubnodeIntercepter intercepter = quorumPeerAspect.getIntercepter(threadId);

        AtomicInteger msgsInQueuedRequests = intercepter.getMsgsInQueue();
        final int toBeSyncNum = msgsInQueuedRequests.get();
        LOG.debug("--------------My queuedRequests has {} element left after takeOrPollFromQueueRequest. " +
                "msgsInQueuedRequests has {} element.", queue.size(), toBeSyncNum);
//        if (toBeSyncNum > 0){
//            LOG.debug("------Using take() and be blocked just now");
//        } else {
//            LOG.debug("------Using poll() and flush now");
//            if (request == null){
//                LOG.debug("------It's not a request! Using poll() and flush now");
//                return;
//            }
//        }
        if (request == null){
            LOG.debug("------It's not a request! Using poll() and flush now");
            return;
        }
        if (request instanceof Request) {
//            this.request = (Request) request;
            LOG.debug("It's a request!");
            final String payload = quorumPeerAspect.constructSyncRequest((Request) request);
            final int type =  ((Request) request).type;
            switch (type) {
                case ZooDefs.OpCode.notification:
                case ZooDefs.OpCode.create:
                case ZooDefs.OpCode.delete:
                case ZooDefs.OpCode.createSession:
                case ZooDefs.OpCode.exists:
                case ZooDefs.OpCode.check:
                case ZooDefs.OpCode.multi:
                case ZooDefs.OpCode.sync:
                case ZooDefs.OpCode.getACL:
                case ZooDefs.OpCode.setACL:
                case ZooDefs.OpCode.getChildren:
                case ZooDefs.OpCode.getChildren2:
                case ZooDefs.OpCode.ping:
                case ZooDefs.OpCode.closeSession:
                case ZooDefs.OpCode.setWatches:
                    LOG.debug("---------Taking the request ({}) from queued requests. Won't intercept.", payload);
                    msgsInQueuedRequests.decrementAndGet();
                    return;
                default:
            };
            try {
                int subnodeId;
                try{
                    subnodeId = intercepter.getSubnodeId();
                } catch (RuntimeException e) {
                    LOG.debug("--------catch exception when acquiring subnode id: {}", e.toString());
                    throw new RuntimeException(e);
                }

                // before offerMessage: increase sendingSubnodeNum
                quorumPeerAspect.setSubnodeSending();
                int lastSyncRequestId = intercepter.getTestingService().logRequestMessage(subnodeId, payload, type);
                LOG.debug("lastSyncRequestId = {}", lastSyncRequestId);
                // after offerMessage: decrease sendingSubnodeNum and shutdown this node if sendingSubnodeNum == 0
                quorumPeerAspect.postSend(subnodeId, lastSyncRequestId);
            } catch (RemoteException e) {
                e.printStackTrace();
            }
            // TODO: Here to decrement. Where to increment? >> Pre
//            msgsInQueuedRequests.decrementAndGet();
        }
    }

//    // TODO: catch args, check request type
//    pointcut addToQueuedRequests():
//            execution(void SyncRequestProcessor.processRequest(Request));
//
//    after() returning: addToQueuedRequests() {
//        msgsInQueuedRequests.incrementAndGet();
//        LOG.debug("----------addToQueuedRequests(). msgsInQueuedRequests.size: {}", msgsInQueuedRequests.get());
//    }

}
