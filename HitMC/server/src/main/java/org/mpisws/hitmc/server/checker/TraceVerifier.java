package org.mpisws.hitmc.server.checker;

import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.statistics.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TraceVerifier implements Verifier{
    private static final Logger LOG = LoggerFactory.getLogger(TraceVerifier.class);

    private final TestingService testingService;
    private final Statistics statistics;
    private Integer traceLen;
    private Integer executedStep;

    // TODO: collect all verification statistics of a trace
    // all Match  & exits Failure


    public TraceVerifier(final TestingService testingService, Statistics statistics) {
        this.testingService = testingService;
        this.statistics = statistics;
        this.traceLen = null;
        this.executedStep = null;
    }

    public void setTraceLen(Integer traceLen) {
        this.traceLen = traceLen;
    }

    public void setExecutedStep(Integer executedStep) {
        this.executedStep = executedStep;
    }

    @Override
    public boolean verify() {
        String matchModel = "UNMATCHED";
        if (traceLen == null || executedStep == null) {
            matchModel = "UNKNOWN";
            statistics.reportResult("TRACE_EXECUTION:FAILURE:" + matchModel);
            return false;
        } else if (executedStep == traceLen + 1) {
            matchModel = "MATCHED";
            statistics.reportResult("TRACE_EXECUTION:SUCCESS:" + matchModel);
            return true;
        }
        statistics.reportResult("TRACE_EXECUTION:FAILURE:" + matchModel);
        return false;
    }
}
