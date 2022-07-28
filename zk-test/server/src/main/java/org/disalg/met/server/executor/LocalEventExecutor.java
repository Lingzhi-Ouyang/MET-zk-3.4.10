package org.disalg.met.server.executor;

import org.disalg.met.api.MessageType;
import org.disalg.met.api.SubnodeState;
import org.disalg.met.api.SubnodeType;
import org.disalg.met.server.TestingService;
import org.disalg.met.server.event.LocalEvent;
import org.disalg.met.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/***
 * Executor of local event
 */
public class LocalEventExecutor extends BaseEventExecutor{
    private static final Logger LOG = LoggerFactory.getLogger(LocalEventExecutor.class);

    private final TestingService testingService;

    public LocalEventExecutor(final TestingService testingService) {
        this.testingService = testingService;
    }

    @Override
    public boolean execute(final LocalEvent event) throws IOException {
        if (event.isExecuted()) {
            LOG.info("Skipping an executed local event: {}", event.toString());
            return false;
        }
        LOG.debug("Processing request: {}", event.toString());
        releaseRequestProcessor(event);
        testingService.waitAllNodesSteady();
        event.setExecuted();
        LOG.debug("Local event executed: {}\n\n\n", event.toString());
        return true;
    }

    // Should be called while holding a lock on controlMonitor
    /***
     * For sync
     * set SYNC_PROCESSOR / COMMIT_PROCESSOR to PROCESSING
     * @param event
     */
    public void releaseRequestProcessor(final LocalEvent event) {
//        logRequestInFlight = event.getId();
        testingService.setMessageInFlight(event.getId());
        SubnodeType subnodeType = event.getSubnodeType();
        if (SubnodeType.SYNC_PROCESSOR.equals(subnodeType)) {
            final Long zxid = event.getZxid();
            Map<Long, Integer> zxidSyncedMap = testingService.getZxidSyncedMap();
            testingService.getZxidSyncedMap().put(zxid, zxidSyncedMap.getOrDefault(zxid, 0) + 1);
        }
        final Subnode subnode = testingService.getSubnodes().get(event.getSubnodeId());

        // set the corresponding subnode to be PROCESSING
        subnode.setState(SubnodeState.PROCESSING);

        testingService.getControlMonitor().notifyAll();

        // set the next subnode to be PROCESSING
        switch (event.getSubnodeType()) {
            case QUORUM_PEER: // for FollowerProcessSyncMessage
                // for FollowerProcessSyncMessage: wait itself to SENDING state since ACK_NEWLEADER will come later anyway
                // for FollowerProcessCOMMITInSync:
                // FollowerProcessPROPOSALInSync:
                int eventType = event.getType();
                if (eventType == MessageType.DIFF || eventType == MessageType.TRUNC || eventType == MessageType.SNAP) {
                    testingService.getSyncTypeList().set(event.getNodeId(), event.getType());
                }
                testingService.waitSubnodeInSendingState(event.getSubnodeId());
                break;
            case SYNC_PROCESSOR:
//                // interceptor version 1
//                // This if for the implementation not intercepting follower's ack to leader,
//                // which means we assume that after the proposal logs,
//                // the follower will ack to leader successfully
//                if (quorumSynced(event.getZxid())){
//                    // If learnerHandler's COMMIT is not intercepted
////                    testingService.waitQuorumToCommit(event);
//                    testingService.waitAllNodesSteadyAfterQuorumSynced();
//                } else {
//                    testingService.waitAllNodesSteady();
//                }

                // interceptor version 2
                // intercept log event and ack event
                // leader will ack self, which is not intercepted
                // follower will send ACK message to leader, which is intercepted only in follower
//                LOG.debug("wait follower {}'s SYNC thread be SENDING ACK", event.getNodeId());
//                testingService.waitSubnodeInSendingState(subnode.getId()); // this is just for follower
                break;
            case COMMIT_PROCESSOR:
                testingService.waitCommitProcessorDone(event.getId(), event.getNodeId());
                break;
        }
    }




}
