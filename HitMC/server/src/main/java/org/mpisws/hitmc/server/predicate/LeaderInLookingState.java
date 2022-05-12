package org.mpisws.hitmc.server.predicate;

import org.mpisws.hitmc.api.state.LeaderElectionState;
import org.mpisws.hitmc.server.TestingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderInLookingState implements WaitPredicate{

    private static final Logger LOG = LoggerFactory.getLogger(LeaderInLookingState.class);

    private final TestingService testingService;

    private final int leaderId;

    public LeaderInLookingState(final TestingService testingService, final int leaderId) {
        this.testingService = testingService;
        this.leaderId = leaderId;
    }

    @Override
    public boolean isTrue() {
        return testingService.getLeaderElectionStates().get(leaderId) == LeaderElectionState.LOOKING;
    }

    @Override
    public String describe() {
        return "leader " + leaderId +" changes to LOOKING state";
    }
}
