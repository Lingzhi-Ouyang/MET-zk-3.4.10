package org.mpisws.hitmc.server.predicate;

import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.event.ClientRequestEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Wait Predicate for the result of a client request event
 */
public class ResponseForClientRequest implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(ResponseForClientRequest.class);

    private final TestingService testingService;

    private final ClientRequestEvent event;

    public ResponseForClientRequest(final TestingService testingService, final ClientRequestEvent event) {
        this.testingService = testingService;
        this.event = event;
    }

    @Override
    //TODO: need to complete
    public boolean isTrue() {
        return event.getResult() != null;
    }

    @Override
    public String describe() {
        return "response of " + event.toString();
    }
}
