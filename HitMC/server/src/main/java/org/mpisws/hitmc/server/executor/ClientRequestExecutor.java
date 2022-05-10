package org.mpisws.hitmc.server.executor;

import org.mpisws.hitmc.api.NodeStateForClientRequest;
import org.mpisws.hitmc.api.state.ClientRequestType;
import org.mpisws.hitmc.server.event.ClientRequestEvent;
import org.mpisws.hitmc.server.TestingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ClientRequestExecutor extends BaseEventExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(ClientRequestExecutor.class);

    private final TestingService testingService;

    private int count = 5;

    private boolean waitForResponse = false;

    public ClientRequestExecutor(final TestingService testingService) {
        this.testingService = testingService;
    }

    public ClientRequestExecutor(final TestingService testingService, boolean waitForResponse) {
        this.testingService = testingService;
        this.waitForResponse = waitForResponse;
    }

    @Override
    public boolean execute(final ClientRequestEvent event) throws IOException {
        if (event.isExecuted()) {
            LOG.info("Skipping an executed client request event: {}", event.toString());
            return false;
        }
        LOG.debug("Releasing client request event: {}", event.toString());
        releaseClientRequest(event);
        // waitPredicate has moved to the above method

        event.setExecuted();
        LOG.debug("Client request executed: {}", event.toString());
        return true;
    }

    /***
     * The executor of client requests
     * @param event
     */
    public void releaseClientRequest(final ClientRequestEvent event) {
//        if (clientProxy.isStop()) {
//            isClientInitializationDone =false;
//            clientProxy = new ClientProxy();
//            clientProxy.init(false);
//            clientProxy.start();
//            isClientInitializationDone = true;
//        } else {
//            LOG.info("------The client is still running!----");
//        }
        switch (event.getType()) {
            case GET_DATA:
                // TODO: this method should modify related states
//                for (int i = 0 ; i < schedulerConfiguration.getNumNodes(); i++) {
//                    nodeStateForClientRequests.set(i, NodeStateForClientRequest.SET_PROCESSING);
//                }
                testingService.getRequestQueue().offer(event);
                testingService.getControlMonitor().notifyAll();

                if (waitForResponse) {
                    // When we want to get the result immediately
                    testingService.waitResponseForClientRequest(event);
                }
                // Note: the client request event may lead to deadlock easily
                //          when scheduled between some RequestProcessorEvents
                if (count > 0) {
                    final ClientRequestEvent clientRequestEvent = new ClientRequestEvent(testingService.generateEventId(),
                            ClientRequestType.GET_DATA, this);
                    testingService.addEvent(clientRequestEvent);
                    count--;
                }
                testingService.waitAllNodesSteady();
                break;
            case SET_DATA:
                for (int i = 0 ; i < testingService.getSchedulerConfiguration().getNumNodes(); i++) {
                    testingService.getNodeStateForClientRequests().set(i, NodeStateForClientRequest.SET_PROCESSING);
                }

                // TODO: This should set the leader learnerHandlerSender / syncProcessor into PROCESSING state
                // TODO: what if leader does not exist?

                String data = String.valueOf(event.getId());
                event.setData(data);
                testingService.getRequestQueue().offer(event);
                // notifyAll() should be called after related states have been changed
                testingService.getControlMonitor().notifyAll();
                testingService.waitAllNodesSteadyAfterMutation();
                break;
        }
    }
}
