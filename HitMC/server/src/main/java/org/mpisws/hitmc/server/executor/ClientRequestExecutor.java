package org.mpisws.hitmc.server.executor;

import org.mpisws.hitmc.server.event.ClientRequestEvent;
import org.mpisws.hitmc.server.TestingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;

public class ClientRequestExecutor extends BaseEventExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(ClientRequestExecutor.class);

    private final TestingService testingService;

    public ClientRequestExecutor(final TestingService testingService) {
        this.testingService = testingService;
    }

    @Override
    public boolean execute(final ClientRequestEvent event) throws IOException {
        if (event.isExecuted()) {
            LOG.info("Skipping an executed client request event: {}", event.toString());
            return false;
        }
        LOG.debug("Releasing client request event: {}", event.toString());
        testingService.releaseClientRequest(event);
//        if (event.getType() == ClientRequestType.SET_DATA) {
//            scheduler.waitAllNodesSteadyForClientMutation();
//        }
//        testingService.waitAllNodesLogSyncSteady();
        testingService.waitAllNodesSteadyAfterClientRequest();

        // TODO: add later event
//        final ClientRequestEvent clientRequestEvent = new ClientRequestEvent(testingService.generateEventId(),
//                event.getType(), testingService.getClientRequestExecutor());
//        testingService.addEvent(clientRequestEvent);
        event.setExecuted();
        LOG.debug("Client request executed: {}", event.toString());
        return true;
    }
}
