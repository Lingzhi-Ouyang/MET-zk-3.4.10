package org.mpisws.hitmc.server.scheduler;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.mpisws.hitmc.api.MessageType;
import org.mpisws.hitmc.api.SubnodeType;
import org.mpisws.hitmc.api.configuration.SchedulerConfigurationException;
import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.event.Event;
import org.mpisws.hitmc.server.event.FollowerToLeaderMessageEvent;
import org.mpisws.hitmc.server.event.LeaderToFollowerMessageEvent;
import org.mpisws.hitmc.server.event.LocalEvent;
import org.mpisws.hitmc.server.state.Subnode;
import org.mpisws.hitmc.server.statistics.ExternalModelStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;

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

    public Set<Event> getEvents() {
        return events;
    }

    public void clearEvents() {
        events.clear();
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
//                    LOG.debug("trace: {}", trace.toString());
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
        // Only json files will be parsed
        if(filename.startsWith(".") || !filename.endsWith(".json")) {
            return null;
        }
        LOG.debug("Importing trace from file {}", filename);

        // acquire file text
        InputStreamReader read = null;
        try {
            read = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        assert read != null;
        BufferedReader bufferedReader = new BufferedReader(read);
        String lineTxt;
        StringBuffer sb = new StringBuffer();
        while ((lineTxt = bufferedReader.readLine()) != null) {
            sb.append(lineTxt);
        }
        read.close();

        // parse json & store to the EventSequence structure

        String fileTxt = sb.toString().replaceAll("\r\n", "");
        JSONArray jsonArray = new JSONArray();
        if (StringUtils.isNoneBlank(fileTxt)) {
            jsonArray = JSONArray.parseArray(fileTxt);
        }


        // get cluster info
        JSONObject serverInfo = (JSONObject) jsonArray.remove(0);
        int serverNum = (int) serverInfo.get("server_num");
        List<String> serverIds = (List<String>) serverInfo.get("server_id");
        // By default, the node mapping:
        // s0 : id = 2
        // s1 : id = 1
        // s2 : id = 0

        int eventCount = jsonArray.size();
        Trace trace = new Trace(filename, serverNum, serverIds, jsonArray);
        LOG.debug("serverNum: {}, serverId: {}, eventCount: {}, jsonArraySize: {}",
                serverNum, serverIds, eventCount, eventCount);

        Set<String> keys = new HashSet<>();
        List<String> events = new LinkedList<>();
        for (int i = 0; i < eventCount; i++) {
            JSONObject jsonObject = (JSONObject) jsonArray.get(i);
            String key = jsonObject.keySet().iterator().next();
            keys.add(key);
            events.add(key);
        }
        LOG.debug("keySize: {}", keys.size());
        LOG.debug("keys: {}", keys);
        LOG.debug("events: {}", events);

        return trace;
    }

    public Event getNextInternalEvent(String action, int nodeId, int peerId) throws SchedulerConfigurationException {
        // 1. get all enabled events
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
        assert enabled.size() > 0;

        // 2. search specific internal event type
        switch (action) {
            case "LeaderSyncFollower": // send NEWLEADER. for now we pass DIFF / TRUNC. NOTE: SNAP is beyond consideration.
            case "LeaderProcessACKLD": // send UPTODATE
            case "LeaderToFollowerCOMMIT": // SEND COMMIT
                searchLeaderMessage(action, nodeId, peerId, enabled);
                break;
            case "LeaderLogPROPOSAL":
            case "FollowerProcessSyncMessage": // no ACK. process DIFF / TRUNC / SNAP
            case "FollowerProcessPROPOSALInSync": // no reply
            case "FollowerProcessCOMMITInSync": // no reply
            case "FollowerProcessPROPOSAL": // no reply TODO: this is only the first phase
            case "LeaderProcessCOMMIT":
            case "FollowerProcessCOMMIT": // COMMIT
                searchLocalMessage(action, nodeId, enabled);
                break;
            case "FollowerProcessNEWLEADER": // ACK to NEWLEADER
            case "FollowerProcessUPTODATE": // ACK to UPTODATE
                searchFollowerMessage(action, nodeId, peerId, enabled);
                break;
        }

        if ( nextEvent != null){
            LOG.debug("next event exists! {}", nextEvent);
        } else {
            throw new SchedulerConfigurationException();
        }

        nextEventPrepared = false;
        return nextEvent;
    }

    public void searchLeaderMessage(final String action, final int nodeId, final int peerId, List<Event> enabled) {
        for (final Event e : enabled) {
            if (e instanceof LeaderToFollowerMessageEvent) {
                final LeaderToFollowerMessageEvent event = (LeaderToFollowerMessageEvent) e;
                final int receivingNodeId = event.getReceivingNodeId();
                final int sendingSubnodeId = event.getSendingSubnodeId();
                final Subnode sendingSubnode = testingService.getSubnodes().get(sendingSubnodeId);
                final int sendingNodeId = sendingSubnode.getNodeId();
                if (sendingNodeId != nodeId || receivingNodeId != peerId) continue;
                final int type = event.getType();
                switch (type) {
                    case MessageType.NEWLEADER:
                        if (!action.equals("LeaderSyncFollower")) continue;
                        break;
                    case MessageType.UPTODATE:
                        if (!action.equals("LeaderProcessACKLD")) continue;
                        break;
                    case MessageType.COMMIT:
                        if (!action.equals("LeaderToFollowerCOMMIT")) continue;
                        break;
                    default:
                        continue;
                }
                nextEvent = event;
                events.remove(nextEvent);
                break;
            }
        }
    }

    public void searchFollowerMessage(final String action, final int nodeId, final int peerId, List<Event> enabled) {
        for (final Event e : enabled) {
            if (e instanceof FollowerToLeaderMessageEvent) {
                final FollowerToLeaderMessageEvent event = (FollowerToLeaderMessageEvent) e;
                final int receivingNodeId = event.getReceivingNodeId();
                final int sendingSubnodeId = event.getSendingSubnodeId();
                final Subnode sendingSubnode = testingService.getSubnodes().get(sendingSubnodeId);
                final int sendingNodeId = sendingSubnode.getNodeId();
                if (sendingNodeId != nodeId || receivingNodeId != peerId) continue;
                final int type = event.getType(); // this describes the message type that this ACK replies to
                switch (type) {
                    case MessageType.NEWLEADER:
                        if (!action.equals("FollowerProcessNEWLEADER")) continue;
                        break;
                    case MessageType.UPTODATE:
                        if (!action.equals("FollowerProcessUPTODATE")) continue;
                        break;
                    case MessageType.PROPOSAL:
                        if (!action.equals("FollowerProcessPROPOSAL")) continue;
                        break;
                    default:
                        continue;
                }
                nextEvent = event;
                events.remove(nextEvent);
                break;
            }
        }
    }

    public void searchLocalMessage(final String action, final int nodeId, List<Event> enabled) {
        for (final Event e : enabled) {
            if (e instanceof LocalEvent) {
                final LocalEvent event = (LocalEvent) e;
                final int eventNodeId = event.getNodeId();
                if (eventNodeId != nodeId) continue;
                final SubnodeType subnodeType = event.getSubnodeType();
                final int type = event.getType();
                switch (action) {
                    case "LeaderLogPROPOSAL":
                    case "FollowerProcessPROPOSAL":  // LOG_REQUEST . TODO: add interceptor to this composite action
                        if (!subnodeType.equals(SubnodeType.SYNC_PROCESSOR)) continue;
//                        if (type != MessageType.PROPOSAL) continue; // set_data type == 5 not proposal!
                        break;
                    case "FollowerProcessSyncMessage": // no ACK. process DIFF / TRUNC / SNAP /
                    case "FollowerProcessCOMMITInSync": // no reply
                    case "FollowerProcessPROPOSALInSync":
                        if (!subnodeType.equals(SubnodeType.QUORUM_PEER)) continue;
                        break;
                    case "LeaderProcessCOMMIT":
                    case "FollowerProcessCOMMIT": // no reply
                        if (!subnodeType.equals(SubnodeType.COMMIT_PROCESSOR)) continue;
                        break;
                    default:
                        continue;
                }
                nextEvent = event;
                events.remove(nextEvent);
                break;
            }
        }
    }
}
