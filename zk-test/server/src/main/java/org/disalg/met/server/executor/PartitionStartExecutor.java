package org.disalg.met.server.executor;

import org.disalg.met.server.TestingService;
import org.disalg.met.server.event.PartitionStartEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class PartitionStartExecutor extends BaseEventExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionStopExecutor.class);
    private final TestingService testingService;

    //TODO: + partitionBudget configuration
    private int partitionBudget;

    public PartitionStartExecutor(final TestingService testingService, final int partitionBudget) {
        this.testingService = testingService;
        this.partitionBudget = partitionBudget;
    }

    @Override
    public boolean execute(final PartitionStartEvent event) throws IOException {
        boolean truelyExecuted = false;
        if (enablePartition()) {
            startPartition(event.getNode1(), event.getNode2());
            testingService.waitAllNodesSteady();
            partitionBudget--;
            truelyExecuted = true;
        }
        event.setExecuted();
        return truelyExecuted;
    }

    public boolean enablePartition() {
        return partitionBudget > 0;
    }

    /***
     * Called by the partition start executor
     * @return
     */
    public void startPartition(final int node1, final int node2) {
        // PRE_EXECUTION: set unstable state (set STARTING)
//        nodeStates.set(node1, NodeState.STARTING);
//        nodeStates.set(node2, NodeState.STARTING);

        List<List<Boolean>> partitionMap = testingService.getPartitionMap();
        // 2. EXECUTION
        LOG.debug("start partition: {} & {}", node1, node2);
        LOG.debug("before partition: {}, {}, {}", partitionMap.get(0), partitionMap.get(1), partitionMap.get(2));
        partitionMap.get(node1).set(node2, true);
        partitionMap.get(node2).set(node1, true);
        LOG.debug("after partition: {}, {}, {}", partitionMap.get(0), partitionMap.get(1), partitionMap.get(2));


        // wait for the state to be stable (set ONLINE)
//        nodeStates.set(node1, NodeState.ONLINE);
//        nodeStates.set(node2, NodeState.ONLINE);

        testingService.getControlMonitor().notifyAll();
    }
}
