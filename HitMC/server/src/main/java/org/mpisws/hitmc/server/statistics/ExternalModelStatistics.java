package org.mpisws.hitmc.server.statistics;

public class ExternalModelStatistics implements Statistics {

    private int sumEnabledEvents;
    private int maxEnabledEvents;
    private int countEnabledEvents;

    public void reportNumberOfEnabledEvents(final int numEnabledEvents) {
        sumEnabledEvents += numEnabledEvents;
        maxEnabledEvents = Math.max(maxEnabledEvents, numEnabledEvents);
        countEnabledEvents++;
    }

    private long startTime;

    @Override
    public void startTimer() {
        startTime = System.currentTimeMillis();
    }

    private long endTime;

    @Override
    public void endTimer() {
        endTime = System.currentTimeMillis();
    }

    private String property;
    private String result;

    @Override
    public void reportResult(final String result) {
        String[] arr = result.split(":");
        assert arr.length == 2;
        this.property = arr[0];
        this.result = arr[1];
    }

    private int totalExecutedEvents;

    @Override
    public void reportTotalExecutedEvents(final int totalExecutedEvents) {
        this.totalExecutedEvents = totalExecutedEvents;
    }

    private long seed;

    @Override
    public void reportRandomSeed(final long seed) {
        this.seed = seed;
    }

    @Override
    public String toString() {
        final double avgEnabledEvents = ((double) sumEnabledEvents) / countEnabledEvents;
        final long totalTime = endTime - startTime;
        return "ExternalModelStatistics{" +
                "\n, totalEvents = " + totalExecutedEvents +
                "\n, averageEnabledEvents = " + String.format("%.3f", avgEnabledEvents) +
                "\n, maxEnabledEvents = " + maxEnabledEvents +
                "\n, totalTime = " + totalTime + " ms" +
                "\n, checkingProperty = " + property +
                "\n, result = " + result +
                "\n}";
    }
}
