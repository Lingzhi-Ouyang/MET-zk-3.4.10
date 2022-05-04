package org.mpisws.hitmc.server.predicate;

import org.mpisws.hitmc.api.NodeState;
import org.mpisws.hitmc.server.TestingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Wait Predicate for the release of a message during election
 * When a node is stopping, this predicate will immediately be set true
 */
public class MessageReleased implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(MessageReleased.class);

    private final TestingService testingService;

    private final int msgId;
    private final int sendingNodeId;

    public MessageReleased(final TestingService testingService, int msgId, int sendingNodeId) {
        this.testingService = testingService;
        this.msgId = msgId;
        this.sendingNodeId = sendingNodeId;
    }

    @Override
    public boolean isTrue() {
        LOG.debug("-------------message released: {}", msgId);
        return testingService.getMessageInFlight() == msgId ||
                NodeState.STOPPING.equals(testingService.getNodeStates().get(sendingNodeId));
    }

    @Override
    public String describe() {
        return "release of message " + msgId + " sent by node " + sendingNodeId;
    }
}

