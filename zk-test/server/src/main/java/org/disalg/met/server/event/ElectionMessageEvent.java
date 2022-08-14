package org.disalg.met.server.event;

import org.disalg.met.api.TestingDef;
import org.disalg.met.server.executor.ElectionMessageExecutor;

import java.io.IOException;

public class ElectionMessageEvent extends AbstractEvent {

    private final int sendingSubnodeId;
    private final int receivingNodeId;
    private final String payload;
    private int flag;

    public ElectionMessageEvent(final int id, final int sendingSubnodeId, final int receivingNodeId, final String payload, final ElectionMessageExecutor electionMessageExecutor) {
        super(id, electionMessageExecutor);
        this.sendingSubnodeId = sendingSubnodeId;
        this.receivingNodeId = receivingNodeId;
        this.payload = payload;
        this.flag = TestingDef.RetCode.NOT_INTERCEPTED;
    }

    public int getSendingSubnodeId() {
        return sendingSubnodeId;
    }

    public int getReceivingNodeId() {
        return receivingNodeId;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    @Override
    public boolean execute() throws IOException {
        return getEventExecutor().execute(this);
    }

    @Override
    public String toString() {
        return "ElectionMessageEvent{" +
                "id=" + getId() +
                ", predecessors=" + getDirectPredecessorsString() +
                ", " + payload +
                getLabelString() +
                '}';
    }
}
