package org.disalg.met.server.checker;

import org.disalg.met.server.TestingService;
import org.disalg.met.server.statistics.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        String passTest = testingService.tracePassed ? "PASS" : "FAILURE";

        String matchModel = "UNMATCHED";
        if (traceLen == null || executedStep == null) {
            matchModel = "UNKNOWN";
        } else if (executedStep >= traceLen) {
            matchModel = "MATCHED";
        }
        if (matchModel.equals("UNMATCHED")) {
            testingService.traceMatched = false;
        }
        statistics.reportResult("TRACE_EXECUTION:" + passTest + ":" + matchModel);

        return testingService.traceMatched && testingService.tracePassed;
    }
}
