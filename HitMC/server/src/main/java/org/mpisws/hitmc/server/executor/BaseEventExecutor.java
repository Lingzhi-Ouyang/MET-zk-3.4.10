package org.mpisws.hitmc.server.executor;

import org.mpisws.hitmc.server.event.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;

public class BaseEventExecutor {

    public boolean execute(final NodeCrashEvent event) throws IOException {
        throw new NotImplementedException();
    }

    public boolean execute(final NodeStartEvent event) throws IOException {
        throw new NotImplementedException();
    }

    public boolean execute(final MessageEvent event) throws IOException {
        throw new NotImplementedException();
    }

    public boolean execute(final ClientRequestEvent event) throws IOException {
        throw new NotImplementedException();
    }

    public boolean execute(final RequestEvent event) throws IOException {
        throw new NotImplementedException();
    }

    public boolean execute(final LearnerHandlerMessageEvent event) throws IOException {
        throw new NotImplementedException();
    }

    public boolean execute(final PartitionStartEvent event) throws IOException {
        throw new NotImplementedException();
    }

    public boolean execute(final PartitionStopEvent event) throws IOException {
        throw new NotImplementedException();
    }
}
