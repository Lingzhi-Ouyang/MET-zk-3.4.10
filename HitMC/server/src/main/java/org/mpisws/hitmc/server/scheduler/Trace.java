package org.mpisws.hitmc.server.scheduler;

import java.util.ArrayList;
import java.util.List;

public class Trace {

    private final String traceName;
    private final List<String> executionSteps = new ArrayList<>();
    private int currentIdx;
    private int stepCount;

    public Trace(String traceName) {
        this.traceName = traceName;
        this.currentIdx = -1;
        this.stepCount = 0;
    }

    public String getTraceName() {
        return traceName;
    }

    public int getStepNum() {
        return stepCount;
    }

    public void addStep(String event) {
        executionSteps.add(event);
        stepCount++;
    }

    public String nextStep() {
        this.currentIdx++;
        assert currentIdx < stepCount;
        return executionSteps.get(currentIdx);
    }

    @Override
    public String toString() {
        return "Trace{" +
                "traceName='" + traceName + '\'' +
                ", executionSteps=" + executionSteps +
                ", currentIdx=" + currentIdx +
                ", stepCount=" + stepCount +
                '}';
    }
}
