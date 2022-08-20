package org.disalg.met.server.event;

import org.disalg.met.api.TestingDef;
import org.disalg.met.server.executor.ElectionMessageExecutor;

import java.io.IOException;

public class ElectionMessageEvent extends AbstractEvent {

    private final int sendingSubnodeId;
    private final int receivingNodeId;
    private final String payload;

    public ElectionMessageEvent(final int id, final int sendingSubnodeId, final int receivingNodeId, final String payload, final ElectionMessageExecutor electionMessageExecutor) {
        super(id, electionMessageExecutor);
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
        return "ElectionMessageEvent{" +
                "id=" + getId() +
                ", flag=" + getFlag() +
                ", predecessors=" + getDirectPredecessorsString() +
                ", " + payload +
                getLabelString() +
                '}';
    }
}
