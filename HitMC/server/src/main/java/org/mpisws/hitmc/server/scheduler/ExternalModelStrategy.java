package org.mpisws.hitmc.server.scheduler;

import org.mpisws.hitmc.api.configuration.SchedulerConfigurationException;
import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.event.Event;
import org.mpisws.hitmc.server.statistics.ExternalModelStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.SchedulingException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class ExternalModelStrategy implements SchedulingStrategy{

    private static final Logger LOG = LoggerFactory.getLogger(ExternalModelStrategy.class);

    private final TestingService testingService;

    private final Random random;

    private File dir;
    private File[] files;
    private List<Trace> traces = new LinkedList<>();
    private int count = 0;
    private Trace currentTrace = null;

    private boolean nextEventPrepared = false;
    private Event nextEvent = null;
    private final Set<Event> events = new HashSet<>();

    private final ExternalModelStatistics statistics;

    public ExternalModelStrategy(TestingService testingService, Random random, File dir, final ExternalModelStatistics statistics) throws SchedulerConfigurationException {
        this.testingService = testingService;
        this.random = random;
        this.dir = dir;
        this.files = new File(String.valueOf(dir)).listFiles();
        assert files != null;
        this.statistics = statistics;
        load();
    }

    public int getTracesNum() {
        return count;
    }

    public Trace getCurrentTrace(final int idx) {
        assert idx < count;
        currentTrace = traces.get(idx);
        return currentTrace;
    }

    @Override
    public void add(final Event event) {
        LOG.debug("Adding event: {}", event.toString());
        events.add(event);
        if (nextEventPrepared && nextEvent == null) {
            nextEventPrepared = false;
        }
    }

    @Override
    public void remove(Event event) {
        LOG.debug("Removing event: {}", event.toString());
        events.remove(event);
        if (nextEventPrepared) {
            nextEventPrepared = false;
        }
    }

    @Override
    public boolean hasNextEvent() {
        if (!nextEventPrepared) {
            try {
                prepareNextEvent();
            } catch (SchedulerConfigurationException e) {
                LOG.error("Error while preparing next event from trace {}", currentTrace);
                e.printStackTrace();
            }
        }
        return nextEvent != null;
    }

    @Override
    public Event nextEvent() {
        if (!nextEventPrepared) {
            try {
                prepareNextEvent();
            } catch (SchedulerConfigurationException e) {
                LOG.error("Error while preparing next event from trace {}", currentTrace);
                e.printStackTrace();
                return null;
            }
        }
        nextEventPrepared = false;
        LOG.debug("nextEvent: {}", nextEvent.toString());
        return nextEvent;
    }

    private void prepareNextEvent() throws SchedulerConfigurationException {
        final List<Event> enabled = new ArrayList<>();
        LOG.debug("prepareNextEvent: events.size: {}", events.size());
        for (final Event event : events) {
            if (event.isEnabled()) {
                LOG.debug("enabled : {}", event.toString());
                enabled.add(event);
            }
        }
        statistics.reportNumberOfEnabledEvents(enabled.size());

        nextEvent = null;

//        if (enabled.size() > 0) {
//            Event event = getNextEventFromModel();
//            if ( event != null && eventMatched(event)){
//                LOG.debug("next event exists! {}", event);
//                nextEvent = event;
//                events.remove(nextEvent);
//            } else {
//                throw new SchedulerConfigurationException();
//            }
//        }
        if (enabled.size() > 0) {
            final int i = random.nextInt(enabled.size());
            nextEvent = enabled.get(i);
            events.remove(nextEvent);
        }
        nextEventPrepared = true;
    }

    public void load() throws SchedulerConfigurationException {
        LOG.debug("Loading traces from files");
        try {
            for (File file : files) {
                if (file.isFile() && file.exists()) {
                    Trace trace = importTrace(file);
                    if (null == trace) continue;
                    traces.add(trace);
                    count++;
                    LOG.debug("trace: {}", trace.toString());
                } else {
                    LOG.debug("file does not exists! ");
                }
            }
            assert count == traces.size();
        } catch (final IOException e) {
            LOG.error("Error while loading execution data from {}", dir);
            throw new SchedulerConfigurationException(e);
        }
    }

    public Trace importTrace(File file) throws IOException {
        String filename = file.getName();
        if(filename.startsWith(".")) {
            return null;
        }
        LOG.debug("Importing trace from file {}", filename);
        InputStreamReader read = null;
        try {
            read = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        assert read != null;
        BufferedReader bufferedReader = new BufferedReader(read);
        Trace trace = new Trace(filename);
        String lineTxt;
        while ((lineTxt = bufferedReader.readLine()) != null) {
            trace.addStep(lineTxt);
            String[] lineArr = lineTxt.split(" ");
            int len = lineArr.length;
            LOG.debug(lineTxt);
        }
        read.close();
        return trace;
    }

    public Event getNextEventFromModel(String line) {
        String lineTxt = currentTrace.nextStep();
        String[] lineArr = lineTxt.split(" ");
        int len = lineArr.length;
        LOG.debug(lineTxt);
        String action = lineArr[0];
        switch (action) {
            case "ELECTION":
                break;
            case "LOG_REQUEST":
                break;
            case "COMMIT":
                break;
            case "NODE_CRASH":
                break;
            case "NODE_START":
                break;
            case "PARTITION_START":
                break;
            case "PARTITION_STOP":
                break;
            case "ESTABLISH_SESSION":
                break;
            case "GET_DATA":
                break;
            case "SET_DATA":
                break;
        }
        return null;
    }

    private boolean eventMatched(Event event) {
        return false;
    }


}
