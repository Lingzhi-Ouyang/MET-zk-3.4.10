package org.mpisws.hitmc.server.event;

import org.mpisws.hitmc.api.SubnodeType;
import org.mpisws.hitmc.server.executor.LocalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/***
 * This class describes the local event of a node, such as
 *  -> log proposal to disk
 *  -> commit
 *  -> follower process DIFF / TRUNC / SNAP
 */
public class LocalEvent extends AbstractEvent{
    private static final Logger LOG = LoggerFactory.getLogger(LocalEvent.class);

    private final int nodeId;
    private final int subnodeId;
    private final SubnodeType subnodeType;
    private final String payload;
    private final long zxid;

    public LocalEvent(final int id, final int nodeId, final int subnodeId, final SubnodeType subnodeType,
                      final String payload, final long zxid, final LocalEventExecutor localEventExecutor) {
        super(id, localEventExecutor);
        this.nodeId = nodeId;
        this.subnodeId = subnodeId;
        this.subnodeType = subnodeType;
        this.payload = payload;
        this.zxid = zxid;
    }

    public int getNodeId() {
        return nodeId;
    }

    public int getSubnodeId() { return subnodeId; }

    public SubnodeType getSubnodeType() {
        return subnodeType;
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
        return "LocalEvent{" +
                "id=" + getId() +
                ", nodeId=" + nodeId +
                ", subnodeId=" + subnodeId +
                ", subnodeType=" + subnodeType +
                ", " + payload +
                getLabelString() +
                '}';
    }
}
