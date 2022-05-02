package org.mpisws.hitmc.server.statistics;

public interface Statistics {

    void startTimer();

    void endTimer();

    void reportTotalExecutedEvents(int totalExecutedEvents);

    void reportResult(String result);

    void reportRandomSeed(long seed);

}
