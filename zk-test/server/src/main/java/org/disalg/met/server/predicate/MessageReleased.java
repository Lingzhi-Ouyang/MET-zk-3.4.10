package org.disalg.met.server.predicate;

import org.disalg.met.api.NodeState;
import org.disalg.met.server.TestingService;
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
    private final Integer receivingNodeId;

    public MessageReleased(final TestingService testingService, int msgId, int sendingNodeId) {
        this.testingService = testingService;
        this.msgId = msgId;
        this.sendingNodeId = sendingNodeId;
        this.receivingNodeId = null;
    }

    public MessageReleased(final TestingService testingService, int msgId, int sendingNodeId, int receivingNodeId) {
        this.testingService = testingService;
        this.msgId = msgId;
        this.sendingNodeId = sendingNodeId;
        this.receivingNodeId = receivingNodeId;
    }



    @Override
    public boolean isTrue() {
        if (receivingNodeId == null) {
            return testingService.getMessageInFlight() == msgId ||
                    NodeState.STOPPING.equals(testingService.getNodeStates().get(sendingNodeId));
        } else {
            return testingService.getMessageInFlight() == msgId ||
                    NodeState.STOPPING.equals(testingService.getNodeStates().get(sendingNodeId)) ||
                    NodeState.STOPPING.equals(testingService.getNodeStates().get(receivingNodeId));
        }
    }

    @Override
    public String describe() {
        return "release of message " + msgId + " sent by node " + sendingNodeId;
    }
}

