package org.mpisws.hitmc.server.event;

import org.mpisws.hitmc.api.MessageType;
import org.mpisws.hitmc.server.executor.FollowerToLeaderMessageExecutor;

import java.io.IOException;

public class FollowerToLeaderMessageEvent extends AbstractEvent {
    private final int sendingSubnodeId;
    private final int receivingNodeId;
    private final String payload;
    private final int type; // this describes the message type that this ACK replies to
    private final long zxid;

    public FollowerToLeaderMessageEvent(final int id,
                                        final int sendingSubnodeId,
                                        final int receivingNodeId,
                                        final int type,
                                        final long zxid,
                                        final String payload,
                                        final FollowerToLeaderMessageExecutor messageExecutor) {
        super(id, messageExecutor);
        this.sendingSubnodeId = sendingSubnodeId;
        this.receivingNodeId = receivingNodeId;
        this.payload = payload;
        this.type = type;
        this.zxid = zxid;
    }

    public int getSendingSubnodeId() {
        return sendingSubnodeId;
    }

    public int getReceivingNodeId() {
        return receivingNodeId;
    }

    public int getType() {
        return type;
    }

    public long getZxid() {
        return zxid;
    }

    @Override
    public boolean execute() throws IOException {
        return getEventExecutor().execute(this);
    }

    @Override
    public String toString() {
        String action = "ACKto";
        switch (type) {
            case MessageType.NEWLEADER:
                action += "NEWLEADER";
                break;
            case MessageType.UPTODATE:
                action += "UPTODATE";
                break;
            case MessageType.PROPOSAL:
                action += "PROPOSAL";
                break;
        }
        return action + "{" +
                "id=" + getId() +
                ", receivingNode=" + receivingNodeId +
                ", predecessors=" + getDirectPredecessorsString() +
                ", ackMessageType=" + type +
                ", " + payload +
                getLabelString() +
                '}';
    }
}