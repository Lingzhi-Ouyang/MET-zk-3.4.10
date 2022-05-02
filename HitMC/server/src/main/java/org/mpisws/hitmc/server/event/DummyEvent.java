package org.mpisws.hitmc.server.event;

public class DummyEvent extends AbstractEvent {

    public DummyEvent() {
        super(-1, null);
    }

    @Override
    public boolean execute() {
        setExecuted();
        return true;
    }
}
