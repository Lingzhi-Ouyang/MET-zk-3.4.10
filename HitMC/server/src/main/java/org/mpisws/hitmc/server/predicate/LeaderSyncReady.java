package org.mpisws.hitmc.server.predicate;

import org.mpisws.hitmc.api.NodeState;
import org.mpisws.hitmc.server.TestingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class LeaderSyncReady implements WaitPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderSyncReady.class);

    private final TestingService testingService;

    private final int leaderId;
    private final List<Integer> peers;

    public LeaderSyncReady(final TestingService testingService,
                           final int leaderId,
                           final List<Integer> peers) {
        this.testingService = testingService;
        this.leaderId = leaderId;
        this.peers = peers;
    }

    @Override
    public boolean isTrue() {
        return testingService.getLeaderSyncFollowerCountMap().get(leaderId) == 0;
    }

    @Override
    public String describe() {
        return " Leader " + leaderId + " sync ready with peers: " + peers;
    }

}
