package org.mpisws.hitmc.server.predicate;

import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.event.ClientRequestEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientSessionReady implements WaitPredicate{
    private static final Logger LOG = LoggerFactory.getLogger(ClientSessionReady.class);

    private final TestingService testingService;

    public ClientSessionReady(final TestingService testingService) {
        this.testingService = testingService;
    }

    @Override
    //TODO: need to complete
    public boolean isTrue() {
        return testingService.getClientProxy().isReady();
    }

    @Override
    public String describe() {
        return "client session ready";
    }
}
