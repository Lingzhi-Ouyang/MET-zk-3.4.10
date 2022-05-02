package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.server.Request;
import org.mpisws.hitmc.api.SubnodeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/***
 * For client requests that go to the follower nodes
 */
//TODO: is this class necessary?
public aspect FollowerRequestProcessorAspect {
    private static final Logger LOG = LoggerFactory.getLogger(FollowerRequestProcessorAspect.class);

    private final QuorumPeerAspect quorumPeerAspect = QuorumPeerAspect.aspectOf();

    // Intercept starting the FollowerRequestProcessor thread

    pointcut runFollowerRequestProcessor(): execution(* FollowerRequestProcessor.run());

    before(): runFollowerRequestProcessor() {
        LOG.debug("-------Thread {}: {}------",Thread.currentThread().getId(), Thread.currentThread().getName());
        LOG.debug("before runFollowerRequestProcessor");
        quorumPeerAspect.registerSubnode(
                Thread.currentThread().getId(), Thread.currentThread().getName(), SubnodeType.FOLLOWER_PROCESSOR);

    }

    after(): runFollowerRequestProcessor() {
        LOG.debug("after runFollowerRequestProcessor");
        quorumPeerAspect.deregisterSubnode(Thread.currentThread().getId());
    }

    // Intercept client request within FollowerRequestProcessor
    pointcut takeOrPollFromQueue(LinkedBlockingQueue queue):
            within(FollowerRequestProcessor)
                    && (call(* LinkedBlockingQueue.take())
                    || call(* LinkedBlockingQueue.poll()))
                    && target(queue);

    before(final LinkedBlockingQueue queue): takeOrPollFromQueue(queue) {
        // TODO: Aspect of aspect
        final long threadId = Thread.currentThread().getId();
        final String threadName = Thread.currentThread().getName();
        LOG.debug("before advice of FollowerRequestProcessor-------Thread: {}, {}------", threadId, threadName);

        QuorumPeerAspect.SubnodeIntercepter intercepter = quorumPeerAspect.getIntercepter(threadId);
        int subnodeId;
        try{
            subnodeId = intercepter.getSubnodeId();
        } catch (RuntimeException e) {
            LOG.debug("--------catch exception: {}", e.toString());
            throw new RuntimeException(e);
        }
        LOG.debug("--------------My FollowerRequestProcessor queuedRequests has {} element. msgsInQueuedRequests has {}.",
                queue.size(), intercepter.getMsgsInQueue().get());
//        if (msgsInQueuedRequests.get() == 0) {
        if (queue.isEmpty()) {
            // Going to block here. Better notify the scheduler
            LOG.debug("--------------Checked! My FollowerRequestProcessor queuedRequests has {} element. Go to RECEIVING state." +
                    " Will be blocked until some request enqueues", queue.size());
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
        LOG.debug("after advice of FollowerRequestProcessor-------Thread: {}, {}------", threadId, threadName);

        QuorumPeerAspect.SubnodeIntercepter intercepter = quorumPeerAspect.getIntercepter(threadId);
        int subnodeId;
        try{
            subnodeId = intercepter.getSubnodeId();
        } catch (RuntimeException e) {
            LOG.debug("--------catch exception: {}", e.toString());
            throw new RuntimeException(e);
        }
        AtomicInteger msgsInQueuedRequests = intercepter.getMsgsInQueue();
        final int toBeTransferredNum = msgsInQueuedRequests.get();
        LOG.debug("--------------My FollowerRequestProcessor queuedRequests has {} element left after takeOrPollFromQueueRequest. " +
                "msgsInQueuedRequests has {} element.", queue.size(), toBeTransferredNum);
//        if (toBeTransferredNum > 0){
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
//            try {
//                // before offerMessage: set workerReceiverSending to true
//                quorumPeerAspect.setSubnodeSending();
//                int lastSyncRequestId = intercepter.getTestingService().logRequestMessage(subnodeId, payload, type);
//                LOG.debug("lastSyncRequestId = {}", lastSyncRequestId);
//                // after offerMessage: set workerReceiverSending to false and shutdown this subnode if needed
//                quorumPeerAspect.postSend(subnodeId, lastSyncRequestId);
//            } catch (RemoteException e) {
//                e.printStackTrace();
//            }
            // TODO: Here to decrement. Where to increment? >> Pre
//            msgsInQueuedRequests.decrementAndGet();
        }
    }


}
