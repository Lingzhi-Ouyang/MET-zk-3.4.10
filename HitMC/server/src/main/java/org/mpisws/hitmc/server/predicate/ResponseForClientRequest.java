package org.mpisws.hitmc.server.predicate;

import org.mpisws.hitmc.server.event.ClientRequestEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * Wait Predicate for the end of a client request event
 */
public class ResponseForClientRequest implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(ResponseForClientRequest.class);

    private final ClientRequestEvent event;

    public ResponseForClientRequest(final ClientRequestEvent event) {
        this.event = event;
    }

    @Override
    public boolean isTrue() {
        return event.getResult() != null;
    }

    @Override
    public String describe() {
        return "response of " + event.toString();
    }
}
