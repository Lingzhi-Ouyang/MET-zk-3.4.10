package org.mpisws.hitmc.server.scheduler;

import org.mpisws.hitmc.server.event.Event;

public interface SchedulingStrategy {

    void add(Event event);

    void remove(Event event);

    boolean hasNextEvent();

    Event nextEvent();

}
