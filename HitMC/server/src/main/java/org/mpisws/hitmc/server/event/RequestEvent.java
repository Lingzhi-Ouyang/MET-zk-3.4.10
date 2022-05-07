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
    private final SubnodeType subnodeType;
    private final String payload;

    public RequestEvent(final int id, final int nodeId, final int subnodeId, final SubnodeType subnodeType,
                        final String payload, final RequestProcessorExecutor requestProcessorExecutor) {
        super(id, requestProcessorExecutor);
        this.nodeId = nodeId;
        this.subnodeId = subnodeId;
        this.subnodeType = subnodeType;
        this.payload = payload;
    }

    public int getNodeId() {
        return nodeId;
    }

    public int getSubnodeId() { return subnodeId; }

    public SubnodeType getSubnodeType() {
        return subnodeType;
    }

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
                ", subnodeType=" + subnodeType +
                ", " + payload +
                getLabelString() +
                '}';
    }
}
