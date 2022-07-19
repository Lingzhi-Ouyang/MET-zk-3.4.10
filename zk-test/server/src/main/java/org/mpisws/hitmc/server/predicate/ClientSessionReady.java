package org.mpisws.hitmc.server.predicate;

import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.event.ClientRequestEvent;
import org.mpisws.hitmc.server.executor.ClientProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ClientSessionReady implements WaitPredicate{
    private static final Logger LOG = LoggerFactory.getLogger(ClientSessionReady.class);

    private final TestingService testingService;

    private final int clientId;

    public ClientSessionReady(final TestingService testingService, final int clientId) {
        this.testingService = testingService;
        this.clientId = clientId;
    }

    @Override
    //TODO: need to complete
    public boolean isTrue() {
        return testingService.getClientProxy(clientId).isReady();
    }

    @Override
    public String describe() {
        return "client session ready";
    }
}
