package org.mpisws.hitmc.server.event;

import org.mpisws.hitmc.server.executor.LearnerHandlerMessageExecutor;

import java.io.IOException;

public class LearnerHandlerMessageEvent extends AbstractEvent{
    private final int sendingSubnodeId;
    private final int receivingNodeId;
    private final String payload;

    public LearnerHandlerMessageEvent(final int id, final int sendingSubnodeId, final int receivingNodeId, final String payload, final LearnerHandlerMessageExecutor messageExecutor) {
        super(id, messageExecutor);
        this.sendingSubnodeId = sendingSubnodeId;
        this.receivingNodeId = receivingNodeId;
        this.payload = payload;
    }

    public int getSendingSubnodeId() {
        return sendingSubnodeId;
    }

    public int getReceivingNodeId() {
        return receivingNodeId;
    }

    @Override
    public boolean execute() throws IOException {
        return getEventExecutor().execute(this);
    }

    @Override
    public String toString() {
        return "LearnerHandlerMessageEvent{" +
                "id=" + getId() +
                ", receivingNodeId=" + receivingNodeId +
                ", predecessors=" + getDirectPredecessorsString() +
                ", " + payload +
                getLabelString() +
                '}';
    }
}
