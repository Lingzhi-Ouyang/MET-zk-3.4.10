package org.mpisws.hitmc.server.checker;

import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.statistics.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class GetDataVerifier implements Verifier{

    private static final Logger LOG = LoggerFactory.getLogger(GetDataVerifier.class);

    private final TestingService testingService;
    private final Statistics statistics;
    private Integer modelResult = null;

    public GetDataVerifier(final TestingService testingService, Statistics statistics) {
        this.testingService = testingService;
        this.statistics = statistics;
    }

    public void setModelResult(Integer modelResult) {
        this.modelResult = modelResult;
    }

    @Override
    public boolean verify() {
        String matchModel = "UNMATCHED";
        List<Integer> returnedZxidList = testingService.getReturnedZxidList();
        final int len = returnedZxidList.size();
        assert len >= 2;
        final int latestOne = returnedZxidList.get(len - 1);
        final int latestSecond = returnedZxidList.get(len - 2);
        boolean result = latestOne >= latestSecond;
        if (this.modelResult == null) {
            matchModel = "UNKNOWN";
        } else if (this.modelResult.equals(latestOne)){
            matchModel = "MATCHED";
        }
        if (result) {
            statistics.reportResult("MONOTONIC_READ:SUCCESS:" + matchModel);
            return true;
        }
        else {
            statistics.reportResult("MONOTONIC_READ:FAILURE:" + matchModel);
            return false;
        }
    }
}
