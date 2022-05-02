package org.mpisws.hitmc.server.event;

import org.mpisws.hitmc.server.executor.LogRequestExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LogRequestEvent extends AbstractEvent{
    private static final Logger LOG = LoggerFactory.getLogger(LogRequestEvent.class);

    private final int nodeId;
    private final int syncSubnodeId;
    private final String payload;

    public LogRequestEvent(final int id, final int nodeId, final int syncSubnodeId, final String payload, final LogRequestExecutor logRequestExecutor) {
        super(id, logRequestExecutor);
        this.nodeId = nodeId;
        this.syncSubnodeId = syncSubnodeId;
        this.payload = payload;
    }

    public int getSyncSubnodeId() { return syncSubnodeId; }

    @Override
    public boolean execute() throws IOException {
        return getEventExecutor().execute(this);
    }

    @Override
    public String toString() {
        return "LogRequestEvent{" +
                "id=" + getId() +
                ", nodeId=" + nodeId +
                ", syncSubnodeId=" + syncSubnodeId +
                ", " + payload +
                getLabelString() +
                '}';
    }
}
