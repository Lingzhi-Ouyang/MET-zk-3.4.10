package org.mpisws.hitmc.server.executor;

import org.mpisws.hitmc.server.event.NodeCrashEvent;
import org.mpisws.hitmc.server.event.NodeStartEvent;
import org.mpisws.hitmc.server.TestingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;

public class NodeStartExecutor extends BaseEventExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(NodeStartExecutor.class);

    private final TestingService testingService;

    private int rebootBudget;

    public NodeStartExecutor(final TestingService testingService, final int rebootBudget) {
        this.testingService = testingService;
        this.rebootBudget = rebootBudget;
    }

    @Override
    public boolean execute(final NodeStartEvent event)  throws IOException {
        boolean truelyExecuted = false;
        if (hasReboots()) {
            final int nodeId = event.getNodeId();
            testingService.setLastNodeStartEvent(nodeId, event);
            testingService.startNode(nodeId);
            testingService.waitAllNodesSteady();
            rebootBudget--;
            if (testingService.getNodeCrashExecutor().hasCrashes()) {
                final NodeCrashEvent nodeCrashEvent = new NodeCrashEvent(testingService.generateEventId(), nodeId, testingService.getNodeCrashExecutor());
                nodeCrashEvent.addDirectPredecessor(event);
                testingService.addEvent(nodeCrashEvent);
            }
            truelyExecuted = true;
        }
        event.setExecuted();
        return truelyExecuted;
    }

    public boolean hasReboots() {
        return rebootBudget > 0;
    }
}
