package org.mpisws.hitmc.server.event;

import org.mpisws.hitmc.api.SubnodeType;
import org.mpisws.hitmc.server.executor.RequestProcessorExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RequestEvent extends AbstractEvent{
    private static final Logger LOG = LoggerFactory.getLogger(RequestEvent.class);

    private final int nodeId;
    private final int subnodeId;
    private final SubnodeType type;
    private final String payload;

    public RequestEvent(final int id, final int nodeId, final int subnodeId, final SubnodeType type,
                        final String payload, final RequestProcessorExecutor requestProcessorExecutor) {
        super(id, requestProcessorExecutor);
        this.nodeId = nodeId;
        this.subnodeId = subnodeId;
        this.type = type;
        this.payload = payload;
    }

    public int getSubnodeId() { return subnodeId; }

    @Override
    public boolean execute() throws IOException {
        return getEventExecutor().execute(this);
    }

    @Override
    public String toString() {
        return "RequestEvent{" +
                "id=" + getId() +
                ", nodeId=" + nodeId +
                ", subnodeId=" + subnodeId +
                ", type=" + type +
                ", " + payload +
                getLabelString() +
                '}';
    }
}
