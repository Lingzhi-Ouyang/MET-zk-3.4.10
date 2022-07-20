package org.disalg.met.server.predicate;

import org.disalg.met.server.TestingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
