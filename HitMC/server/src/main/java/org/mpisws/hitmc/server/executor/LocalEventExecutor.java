package org.mpisws.hitmc.server.executor;

import org.mpisws.hitmc.api.SubnodeState;
import org.mpisws.hitmc.api.SubnodeType;
import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.event.LocalEvent;
import org.mpisws.hitmc.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

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

        // Post-condition
        switch (event.getSubnodeType()) {
            case QUORUM_PEER: // for FollowerProcessSyncMessage
                // for FollowerProcessSyncMessage: wait itself to SENDING state since ACK_NEWLEADER will come later anyway
                testingService.getSyncTypeList().set(event.getNodeId(), event.getType());
                testingService.waitSubnodeInSendingState(event.getSubnodeId());
                testingService.waitAllNodesSteady();
                break;
            case SYNC_PROCESSOR:
                // TODO: this should a composite action including local event and ack message
                // till now we do not yet intercept follower's ack to leader, which means we assume that after the proposal logs,
                // the follower will ack to leader successfully
                if (quorumSynced(event.getZxid())){
                    // If learnerHandler's COMMIT is not intercepted
//                    testingService.waitQuorumToCommit(event);
                    testingService.waitAllNodesSteadyAfterQuorumSynced();
                } else {
                    testingService.waitAllNodesSteady();
                }
                break;
            case COMMIT_PROCESSOR:
                testingService.waitCommitProcessorDone(event.getId(), event.getNodeId());
                testingService.waitAllNodesSteady();
                break;
        }
    }



    public boolean quorumSynced(final long zxid) {
        if (testingService.getZxidSyncedMap().containsKey(zxid)){
            final int count = testingService.getZxidSyncedMap().get(zxid);
            final int nodeNum = testingService.getSchedulerConfiguration().getNumNodes();
            return count > nodeNum / 2;
        }
        return false;
    }
}
