package org.disalg.met.server;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.disalg.met.api.*;
import org.disalg.met.api.configuration.SchedulerConfiguration;
import org.disalg.met.server.checker.CommittedLogVerifier;
import org.disalg.met.server.event.*;
import org.disalg.met.server.executor.*;
import org.disalg.met.server.predicate.*;
import org.disalg.met.server.scheduler.*;
import org.disalg.met.server.statistics.*;
import org.disalg.met.api.configuration.SchedulerConfigurationException;
import org.disalg.met.api.state.ClientRequestType;
import org.disalg.met.api.state.LeaderElectionState;
import org.disalg.met.api.state.Vote;
import org.disalg.met.server.checker.GetDataVerifier;
import org.disalg.met.server.checker.LeaderElectionVerifier;
import org.disalg.met.server.checker.TraceVerifier;
import org.disalg.met.server.state.Subnode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.jws.WebParam;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class TestingService implements TestingRemoteService {

    private static final Logger LOG = LoggerFactory.getLogger(TestingService.class);

    @Autowired
    private SchedulerConfiguration schedulerConfiguration;

    @Autowired
    private Ensemble ensemble;

    private Map<Integer, ClientProxy> clientMap = new HashMap<>();

    // strategy
    private SchedulingStrategy schedulingStrategy;

    // External event executors
    private NodeStartExecutor nodeStartExecutor;
    private NodeCrashExecutor nodeCrashExecutor;
    private ClientRequestExecutor clientRequestWaitingResponseExecutor;
    private ClientRequestExecutor clientRequestExecutor;
    private PartitionStartExecutor partitionStartExecutor;
    private PartitionStopExecutor partitionStopExecutor;

    // Internal event executors
    private ElectionMessageExecutor electionMessageExecutor; // for election
    private LocalEventExecutor localEventExecutor; // for logging
    private LeaderToFollowerMessageExecutor leaderToFollowerMessageExecutor; // for learnerHandler-follower
    private FollowerToLeaderMessageExecutor followerToLeaderMessageExecutor; // for follower-leader

    // statistics
    private Statistics statistics;

    // checker
    private TraceVerifier traceVerifier;
    private LeaderElectionVerifier leaderElectionVerifier;
    private GetDataVerifier getDataVerifier;
    private CommittedLogVerifier committedLogVerifier;

    // writer
    private FileWriter statisticsWriter;
    private FileWriter executionWriter;
    private FileWriter bugReportWriter;
    private FileWriter matchReportWriter;

    private final Object controlMonitor = new Object();

    // For indication of client initialization
    private boolean clientInitializationDone = false;

    // For network partition
    private List<List<Boolean>> partitionMap = new ArrayList<>();

    // node management
    private final List<NodeState> nodeStates = new ArrayList<>();
    private final List<Phase> nodePhases = new ArrayList<>();

    private final List<Subnode> subnodes = new ArrayList<>();
    private final List<Set<Subnode>> subnodeSets = new ArrayList<>();

    private final List<String> followerSocketAddressBook = new ArrayList<>();
    private final List<Integer> followerLearnerHandlerMap = new ArrayList<>();
    private final List<Integer> followerLearnerHandlerSenderMap = new ArrayList<>();

    private final List<NodeStateForClientRequest> nodeStateForClientRequests = new ArrayList<>();

    private final Set<Integer> participants = new HashSet<>();

    /***
     * properties to check
     */

    // record each node's vote for check after election
    private final List<Vote> votes = new ArrayList<>();
    // record each node's role for check after election
    private final List<LeaderElectionState> leaderElectionStates = new ArrayList<>();

    // record each node's lastProcessedZxid
    private final List<Long> lastProcessedZxids = new ArrayList<>();

    // record all nodes' lastProcessedZxid history
    // Note: do not record zxid whose counter is 0 when the epoch >= 1
    // but set 0 as its first record for check convenience
    private final List<List<Long>> allZxidRecords = new ArrayList<>();

    // record the returned values of GetData according to the timeline
    private final List<Integer> returnedDataList = new ArrayList<>();

    // record the committed / applied history (may be visible by clients)
    // Note: do not record zxid whose counter is 0 when the epoch >= 1 (since this zxid does not map to a real commit)
    // ( which is firstly produced by the leader during discovery phase in each epoch)
    // only record the committed zxid during broadcast phase
    // but set 0 as its first record for check convenience
    private final List<Long> lastCommittedZxid = new ArrayList<>();

    // record the number of followers.
    // For post-predicate of ElectionAndDiscovery event
    private final Map<Integer, Integer> leaderSyncFollowerCountMap = new HashMap<>();

    // record the number of nodes that have logged the proposal with specific zxid.
    // For pre-predicate of leader's commit message event
    private final Map<Long, Integer> zxidSyncedMap = new HashMap<>();

    // record the number of nodes that have committed with specific zxid.
    private final Map<Long, Integer> zxidToCommitMap = new HashMap<>();

    // record the sync type : DIFF / TRUNC / SNAP
    private final List<Integer> syncTypeList = new ArrayList<>();

    public boolean traceMatched;
    public boolean tracePassed;

    // event
    private final AtomicInteger eventIdGenerator = new AtomicInteger();

    private final AtomicInteger clientIdGenerator = new AtomicInteger();

    // for event dependency
    private final Map<Integer, ElectionMessageEvent> messageEventMap = new HashMap<>();
//    private final Map<Integer, LeaderToFollowerMessageEvent> followerMessageEventMap = new HashMap<>();

    private final List<NodeStartEvent> lastNodeStartEvents = new ArrayList<>();
    // TODO: firstMessage should be renewed whenever election occurs.
    private final List<Boolean> firstMessage = new ArrayList<>();

    private int messageInFlight;

    private int logRequestInFlight;

    public void setMessageInFlight(int messageInFlight) {
        this.messageInFlight = messageInFlight;
    }

    public List<Phase> getNodePhases() {
        return nodePhases;
    }

    public List<List<Boolean>> getPartitionMap() {
        return partitionMap;
    }

    public void setPartitionMap(List<List<Boolean>> partitionMap) {
        this.partitionMap = partitionMap;
    }

    public List<List<Long>> getAllZxidRecords() {
        return allZxidRecords;
    }

    public List<Integer> getReturnedDataList() {
        return returnedDataList;
    }

    public Map<Long, Integer> getZxidSyncedMap() {
        return zxidSyncedMap;
    }

    public Map<Integer, Integer> getLeaderSyncFollowerCountMap() {
        return leaderSyncFollowerCountMap;
    }

    public Map<Long, Integer> getZxidToCommitMap() {
        return zxidToCommitMap;
    }

    public Object getControlMonitor() {
        return controlMonitor;
    }

    public long getLastProcessedZxid(int nodeId) {
        return lastProcessedZxids.get(nodeId);
    }

    public List<Long> getLastCommittedZxid() {
        return lastCommittedZxid;
    }

    public List<Integer> getSyncTypeList() {
        return syncTypeList;
    }

    public List<NodeStateForClientRequest> getNodeStateForClientRequests() {
        return nodeStateForClientRequests;
    }

    public NodeStateForClientRequest getNodeStateForClientRequests(int nodeId) {
        return nodeStateForClientRequests.get(nodeId);
    }

    public List<NodeState> getNodeStates() {
        return nodeStates;
    }

    public List<Subnode> getSubnodes() {
        return subnodes;
    }

    public List<Set<Subnode>> getSubnodeSets() {
        return subnodeSets;
    }

    public List<String> getFollowerSocketAddressBook() {
        return followerSocketAddressBook;
    }

    public List<Integer> getFollowerLearnerHandlerMap() {
        return followerLearnerHandlerMap;
    }

    public List<Integer> getFollowerLearnerHandlerSenderMap() {
        return followerLearnerHandlerSenderMap;
    }

    public List<Vote> getVotes() {
        return votes;
    }

    public List<LeaderElectionState> getLeaderElectionStates() {
        return leaderElectionStates;
    }

    public List<Boolean> getFirstMessage() {
        return firstMessage;
    }

    public NodeStartExecutor getNodeStartExecutor() {
        return nodeStartExecutor;
    }

    public NodeCrashExecutor getNodeCrashExecutor() {
        return nodeCrashExecutor;
    }

    public ClientProxy getClientProxy(final int clientId) {
        assert clientMap.containsKey(clientId);
        return clientMap.get(clientId);
    }

    public LinkedBlockingQueue<ClientRequestEvent> getRequestQueue(final int clientId) {
        return clientMap.get(clientId).getRequestQueue();
    }

    public SchedulerConfiguration getSchedulerConfiguration() {
        return schedulerConfiguration;
    }

    public void loadConfig(final String[] args) throws SchedulerConfigurationException {
        schedulerConfiguration.load(args);
    }

    /***
     * The core process the scheduler by specifying each step
     * example: steps of ZK-3911
     * @throws SchedulerConfigurationException
     * @throws IOException
     */
    public void start() throws SchedulerConfigurationException, IOException {
        LOG.debug("Starting the testing service");
        long startTime = System.currentTimeMillis();
//        initRemote();

        for (int executionId = 1; executionId <= schedulerConfiguration.getNumExecutions(); ++executionId) {
            ensemble.configureEnsemble(String.valueOf(executionId));
            configureSchedulingStrategy(executionId);

            int totalExecuted = 0;

            executionWriter = new FileWriter(schedulerConfiguration.getWorkingDir() + File.separator
                    + executionId + File.separator + schedulerConfiguration.getExecutionFile());
            statisticsWriter = new FileWriter(schedulerConfiguration.getWorkingDir() + File.separator
                    + executionId + File.separator + schedulerConfiguration.getStatisticsFile());

            // configure the execution before first election
            configureNextExecution();
            // start the ensemble at the beginning of the execution
            ensemble.startEnsemble();
            executionWriter.write("-----Initialization cost time: " + (System.currentTimeMillis() - startTime) + "\n\n");

            // PRE: first election
            totalExecuted = scheduleElection(null, null,null, totalExecuted);

            // Only the success of the last verification will lead to the following test
            // o.w. just report bug

            // 1. trigger DIFF
            int client1 = generateClientId();
//            createClient(client1, true, "127.0.0.1:4002");
            establishSession(client1, true, "127.0.0.1:4002");

            // Restart the leader will disconnect the client session!
//            nodeStartExecutor = new NodeStartExecutor(this, 1);
//            nodeCrashExecutor = new NodeCrashExecutor(this, 1);
//            totalExecuted = triggerDiffByLeaderRestart(totalExecuted, client1);

            nodeStartExecutor = new NodeStartExecutor(this, 2);
            nodeCrashExecutor = new NodeCrashExecutor(this, 2);
            totalExecuted = triggerDiffByFollowersRestart(totalExecuted, client1);

            // Ensure client session still in connection!
            totalExecuted = getDataTest(totalExecuted, client1);

            // 2. phantom read
            // Reconfigure executors
            nodeStartExecutor = new NodeStartExecutor(this, 2);
            nodeCrashExecutor = new NodeCrashExecutor(this, 3);
            totalExecuted = followersRestartAndLeaderCrash(totalExecuted);

            int client2 = generateClientId();
//            createClient(client2, true, "127.0.0.1:4001");
            establishSession(client2, true, "127.0.0.1:4001");
            totalExecuted = getDataTest(totalExecuted, client2);

            for (Integer i: clientMap.keySet()) {
                LOG.debug("shutting down client {}", i);
                clientMap.get(i).shutdown();
            }
            clientMap.clear();
            ensemble.stopEnsemble();

            executionWriter.close();
            statisticsWriter.close();
        }
        LOG.debug("total time: {}" , (System.currentTimeMillis() - startTime));
    }

    /***
     * The core process the scheduler by external model
     * example: steps of ZK-3911
     * @throws SchedulerConfigurationException
     * @throws IOException
     */
    public void startWithExternalModel() throws SchedulerConfigurationException, IOException {
        LOG.debug("Starting the testing service by external model");
        ExternalModelStatistics externalModelStatistics = new ExternalModelStatistics();
        ExternalModelStrategy externalModelStrategy = new ExternalModelStrategy(this, new Random(1), schedulerConfiguration.getTraceDir(), externalModelStatistics);
        bugReportWriter = new FileWriter(schedulerConfiguration.getWorkingDir() + File.separator
                 + schedulerConfiguration.getBugReportFile());
        matchReportWriter = new FileWriter(schedulerConfiguration.getWorkingDir() + File.separator
                + schedulerConfiguration.getMatchReportFile());

        long startTime = System.currentTimeMillis();
        int traceNum = externalModelStrategy.getTracesNum();
        LOG.debug("traceNum: {}", traceNum);
        int bugCount = 0;

        Map<String, Integer> serverIdMap = new HashMap<>(3);
        serverIdMap.put("s0", 2);
        serverIdMap.put("s1", 1);
        serverIdMap.put("s2", 0);

        for (int executionId = 1; executionId <= traceNum; ++executionId) {
            externalModelStrategy.clearEvents();
            schedulingStrategy = externalModelStrategy;
            statistics = externalModelStatistics;

            Trace trace = externalModelStrategy.getCurrentTrace(executionId - 1);
            String traceName = trace.getTraceName();
            int stepCount = trace.getStepCount();
            LOG.info("currentTrace: {}, total steps: {}", trace, stepCount);

            // get event info
            Set<String> keys = new HashSet<>();

            int crnt = 1;
            for (; crnt <= stepCount; ++crnt) {
                JSONObject jsonObject = trace.nextStep();
                String key = jsonObject.keySet().iterator().next();
                keys.add(key);
            }
            LOG.debug("keySize: {}", keys.size());
            LOG.debug("keys: {}", keys);


            ensemble.configureEnsemble(traceName);
//            configureSchedulingStrategy(executionId);

            int totalExecuted = 0;

            executionWriter = new FileWriter(schedulerConfiguration.getWorkingDir() + File.separator
                    + traceName + File.separator + schedulerConfiguration.getExecutionFile());
            statisticsWriter = new FileWriter(schedulerConfiguration.getWorkingDir() + File.separator
                    + traceName + File.separator + schedulerConfiguration.getStatisticsFile());

            // configure the execution before first election
            configureNextExecution();
            // start the ensemble at the beginning of the execution
            ensemble.startEnsemble();
            executionWriter.write("-----Initialization cost time: " + (System.currentTimeMillis() - startTime) + "\n\n");

            // Start the timer for recoding statistics
            statistics.startTimer();

            int currentStep = 0;
            String action = "";
            ModelAction modelAction;
            try {
                synchronized (controlMonitor) {
                    waitAllNodesSteady();
                }
                for (; currentStep < stepCount; ++currentStep) {
                    JSONObject jsonObject = trace.getStep(currentStep);
                    action = jsonObject.keySet().iterator().next();
                    JSONObject elements = jsonObject.getJSONObject(action);
                    LOG.debug("nextStep: {}", jsonObject);
                    LOG.debug("nextStep: {}", action);
                    modelAction = ModelAction.valueOf(action);

                    switch (modelAction) {
                        // external events
                        case NodeCrash:
                            int crashNodeId = serverIdMap.get(elements.getString("nodeId"));
                            totalExecuted = scheduleNodeCrash(crashNodeId, totalExecuted);
                            break;
                        case NodeStart:
                            int startNodeId = serverIdMap.get(elements.getString("nodeId"));
                            totalExecuted = scheduleNodeStart(startNodeId, totalExecuted);
                            break;
                        case PartitionStart:
                            int node1 = serverIdMap.get(elements.getString("nodeId"));
                            int node2 = serverIdMap.get(elements.getString("peerId"));
                            LOG.debug("----PARTITION START: {}, {}", node1, node2);
                            totalExecuted = schedulePartitionStart(node1, node2, totalExecuted);
                            break;
                        case PartitionRecover:
                            int node3 = serverIdMap.get(elements.getString("nodeId"));
                            int node4 = serverIdMap.get(elements.getString("peerId"));
                            LOG.debug("----PARTITION RECOVER: {}, {}", node3, node4);
                            totalExecuted = schedulePartitionStop(node3, node4, totalExecuted);
                            break;
                        case LeaderProcessRequest:  // set data & leader log & send proposal
                            int setDataClientId = elements.getInteger("clientId");
                            int sid2 = serverIdMap.get(elements.getString("nodeId"));
                            Integer data = elements.getInteger("data");
                            String writtenVal = data != null ? data.toString() : null;
                            totalExecuted = scheduleLeaderProcessRequest(externalModelStrategy, setDataClientId, sid2, writtenVal, totalExecuted);
                            break;
                        case ClientGetData:
                            int getDataClientId = elements.getInteger("clientId");
                            int sid3 = serverIdMap.get(elements.getString("nodeId"));
                            Integer returnedData = elements.getInteger("data");
                            totalExecuted = scheduleClientGetData(currentStep, getDataClientId, sid3, returnedData, totalExecuted);
                            break;
                        // internal events
                        // Phase election & discovery
                        case ElectionAndDiscovery:
                            Integer leaderId = serverIdMap.get(elements.getString("nodeId"));
                            JSONArray participants = elements.getJSONArray("peerId");
                            List<Integer> peers = new ArrayList<>();
                            for (Object peer: participants) {
                                peers.add(serverIdMap.get(peer.toString()));
                            }
                            LOG.debug("election leader: {}, other participants: {}", leaderId, peers);
                            totalExecuted = scheduleElectionAndDiscovery(currentStep, leaderId, peers, totalExecuted);
                            break;
                        case LeaderProcessACK: // send COMMIT
                            Integer nodeId1 = serverIdMap.get(elements.getString("nodeId"));
                            Integer peerId1 = serverIdMap.get(elements.getString("peerId"));
                            totalExecuted = scheduleLeaderProcessACK(externalModelStrategy, nodeId1, peerId1, totalExecuted);
                            break;
                        case FollowerProcessPROPOSAL: // followr log & send ACK
//                            Integer fId = serverIdMap.get(elements.getString("nodeId"));
//                            Integer lId = serverIdMap.get(elements.getString("peerId"));
//                            totalExecuted = scheduleFollowerProcessPROPOSAL(externalModelStrategy,
//                                    action, fId, lId, totalExecuted);
//                            break;
                        case FollowerProcessSyncMessage:
                        case LeaderSyncFollower: // send DIFF /TRUNC
                        case LeaderProcessACKLD: // send UPTODATE
                        case FollowerProcessPROPOSALInSync:
                        case FollowerProcessCOMMITInSync:
                        case FollowerProcessNEWLEADER: // send ACK
                        case FollowerProcessUPTODATE: // send ACK
                        case FollowerProcessCOMMIT:
                            Integer nodeId = serverIdMap.get(elements.getString("nodeId"));
                            Integer peerId = serverIdMap.get(elements.getString("peerId"));
                            int retry = 5;
                            totalExecuted = scheduleInternalEventWithWaitingRetry(externalModelStrategy,
                                    modelAction, nodeId, peerId, totalExecuted, retry);
                            break;
                    }
                    committedLogVerifier.verify();
                    statistics.reportCurrentStep("[Step " + currentStep + "]-" + action);
                    statistics.reportTotalExecutedEvents(totalExecuted);
                    statisticsWriter.write(statistics.toString() + "\n\n");
                    LOG.info(statistics.toString() + "\n\n\n\n\n");
                }
            } catch (SchedulerConfigurationException e) {
                LOG.info("SchedulerConfigurationException found when scheduling Trace {} in Step {} / {}. ",
                        traceName, currentStep, stepCount);
                tracePassed = false;
            } finally {
                statistics.endTimer();

                // report statistics of total trace
                LOG.info("setTraceLen: {}, setExecutedStep: {} ", stepCount, currentStep);
                traceVerifier.setTraceLen(stepCount);
                traceVerifier.setExecutedStep(currentStep);
                boolean matchedAndPassed = traceVerifier.verify();
                String info = currentStep >= stepCount ? "COMPLETE" : action;
                statistics.reportCurrentStep("[Step " + currentStep + "]-" + info);
                statistics.reportTotalExecutedEvents(totalExecuted);
                statisticsWriter.write(statistics.toString() + "\n\n");
                LOG.info(statistics.toString() + "\n\n\n\n\n");


                if (!matchedAndPassed) {
                    bugCount++;
                    bugReportWriter.write("Trace: " + traceName + "\n");
                    bugReportWriter.write(statistics.toString() + "\n\n");
                }
                if (!traceMatched) {
                    matchReportWriter.write("Trace: " + traceName + "\n");
                    matchReportWriter.write(statistics.toString() + "\n\n");
                }

                // shutdown clients & servers
                for (Integer i: clientMap.keySet()) {
                    LOG.debug("shutting down client {}", i);
                    clientMap.get(i).shutdown();
                }
                clientMap.clear();
                ensemble.stopEnsemble();

                executionWriter.close();
                statisticsWriter.close();
                bugReportWriter.flush();
                matchReportWriter.flush();
            }
        }
        LOG.debug("total time: {}" , (System.currentTimeMillis() - startTime));
        final int unmatchedCount = TraceVerifier.getUnmatchedCount();
        final int failedCount = TraceVerifier.getFailedCount();
        final float unmatchedRate = (float) unmatchedCount / traceNum ;
        final float failedRate = (float) failedCount / traceNum ;
        final float bugRate = (float) bugCount / traceNum ;
        bugReportWriter.write("TOTAL: " + traceNum + "\n");
        bugReportWriter.write("BUG:\t" + bugCount + "\tNO_BUG:\t" + (traceNum - bugCount) +
                "\tBUG RATE:\t" + bugRate + "\n");
        bugReportWriter.write("UNMATCH:\t" + unmatchedCount + "\tMATCH:\t" + (traceNum - unmatchedCount) +
                "\tUNMATCHED RATE:\t" + unmatchedRate + "\n");
        bugReportWriter.write("FAIL:\t" + failedCount + "\tPASS:\t" + (traceNum - failedCount) +
                "\tFAIL RATE:\t" + failedRate + "\n");
        bugReportWriter.close();
        matchReportWriter.write("UNMATCH:\t" + unmatchedCount + "\tMATCH:\t" + (traceNum - unmatchedCount) +
                "\tUNMATCHED RATE:\t" + unmatchedRate + "\n");
        matchReportWriter.close();
    }

    @Deprecated
    /***
     * The traces are provided by external sequences of events
     * example: steps of ZK-3911
     * Note: The granularity of the trace is coarse with only events with little simple info
     * @throws SchedulerConfigurationException
     * @throws IOException
     */
    public void startWithEventSequence() throws SchedulerConfigurationException, IOException {
        LOG.debug("Starting the testing service by external model");
        ExternalModelStatistics externalModelStatistics = new ExternalModelStatistics();
        EventSequenceStrategy eventSequenceStrategy = new EventSequenceStrategy(this, new Random(1), schedulerConfiguration.getTraceDir(), externalModelStatistics);

        long startTime = System.currentTimeMillis();
        int traceNum = eventSequenceStrategy.getTracesNum();
        LOG.debug("traceNum: {}", traceNum);

        for (int executionId = 1; executionId <= traceNum; ++executionId) {
            eventSequenceStrategy.clearEvents();
            schedulingStrategy = eventSequenceStrategy;
            statistics = externalModelStatistics;

            EventSequence trace = eventSequenceStrategy.getCurrentTrace(executionId - 1);
            String traceName = trace.getTraceName();
            int stepCount = trace.getStepNum();
            LOG.info("currentTrace: {}, total steps: {}", trace, stepCount);

            ensemble.configureEnsemble(traceName);
//            configureSchedulingStrategy(executionId);

            int totalExecuted = 0;

            executionWriter = new FileWriter(schedulerConfiguration.getWorkingDir() + File.separator
                    + traceName + File.separator + schedulerConfiguration.getExecutionFile());
            statisticsWriter = new FileWriter(schedulerConfiguration.getWorkingDir() + File.separator
                    + traceName + File.separator + schedulerConfiguration.getStatisticsFile());

            // configure the execution before first election
            configureNextExecution();
            // start the ensemble at the beginning of the execution
            ensemble.startEnsemble();
            executionWriter.write("-----Initialization cost time: " + (System.currentTimeMillis() - startTime) + "\n\n");

            // Start the timer for recoding statistics
            statistics.startTimer();

            int currentStep = 1;
            String line = null;
            try {
                for (; currentStep <= stepCount; ++currentStep) {
                    line = trace.nextStep();
                    LOG.debug("nextStep: {}", line);

                    String[] lineArr = line.split(" ");
                    int len = lineArr.length;

                    String action = lineArr[0];
                    switch (action) {
                        case "ELECTION":
                            Integer leaderId = len > 1 ? Integer.parseInt(lineArr[1]) : null;
                            LOG.debug("election leader: {}", leaderId);
                            totalExecuted = scheduleElection(currentStep, line, leaderId, totalExecuted);
                            break;
//                    case "LOG_REQUEST":
//                        assert len>=2;
//                        int logNodeId = Integer.parseInt(lineArr[1]);
//                        totalExecuted = scheduleLogRequest(logNodeId, totalExecuted);
//                        break;
//                    case "COMMIT":
//                        assert len>=2;
//                        int commitNodeId = Integer.parseInt(lineArr[1]);
//                        totalExecuted = scheduleCommit(commitNodeId, totalExecuted);
//                        break;
//                    case "SEND_PROPOSAL":
//                    case "SEND_COMMIT":
//                        assert len>=3;
//                        int s1 = Integer.parseInt(lineArr[1]);
//                        int s2 = Integer.parseInt(lineArr[2]);
//                        totalExecuted = scheduleLearnerHandlerMessage(s1, s2, totalExecuted);
//                        break;
                        case "LOG_REQUEST":
                        case "COMMIT":
                        case "SEND_PROPOSAL":
                        case "SEND_COMMIT":
                            totalExecuted = scheduleInternalEventInSequence(eventSequenceStrategy, lineArr, totalExecuted);
                            break;
                        case "NODE_CRASH":
                            assert len==2;
                            int crashNodeId = Integer.parseInt(lineArr[1]);
                            totalExecuted = scheduleNodeCrash(crashNodeId, totalExecuted);
                            break;
                        case "NODE_START":
                            assert len==2;
                            int startNodeId = Integer.parseInt(lineArr[1]);
                            totalExecuted = scheduleNodeStart(startNodeId, totalExecuted);
                            break;
                        case "PARTITION_START":
                            assert len==3;
                            int node1 = Integer.parseInt(lineArr[1]);
                            int node2 = Integer.parseInt(lineArr[2]);
                            LOG.debug("----PARTITION_START: {}, {}", node1, node2);
                            totalExecuted = schedulePartitionStart(node1, node2, totalExecuted);
                            break;
                        case "PARTITION_STOP":
                            assert len==3;
                            int node3 = Integer.parseInt(lineArr[1]);
                            int node4 = Integer.parseInt(lineArr[2]);
                            LOG.debug("----PARTITION_STOP: {}, {}", node3, node4);
                            totalExecuted = schedulePartitionStop(node3, node4, totalExecuted);
                            break;
                        case "ESTABLISH_SESSION":
                            assert len==3;
                            int clientId = Integer.parseInt(lineArr[1]);
                            int serverId = Integer.parseInt(lineArr[2]);
                            String serverAddr = getServerAddr(serverId);
                            LOG.debug("client establish connection with server {}", serverAddr);
                            establishSession(clientId, true, serverAddr);
                            break;
                        case "GET_DATA":
                            assert len>=3;
                            int getDataClientId = Integer.parseInt(lineArr[1]);
                            int sid = Integer.parseInt(lineArr[2]);
                            Integer result = len > 3 ? Integer.parseInt(lineArr[3]) : null;
                            totalExecuted = scheduleGetData(currentStep, line, getDataClientId, sid, result, totalExecuted);
                            break;
                        case "SET_DATA":
                            assert len>=3;
                            int setDataClientId = Integer.parseInt(lineArr[1]);
                            int sid2 = Integer.parseInt(lineArr[2]);
                            String data = len > 3 ? lineArr[3] : null;
                            totalExecuted = scheduleSetData(setDataClientId, sid2, data, totalExecuted);
                            break;
                    }
                }
            } catch (SchedulerConfigurationException e) {
                LOG.info("SchedulerConfigurationException found when scheduling EventSequence {} in Step {} / {}. " +
                                "Complete trace: {}", traceName, currentStep, stepCount, trace);
                tracePassed = false;
            } finally {
                statistics.endTimer();

                // report statistics of total trace
                traceVerifier.setTraceLen(stepCount);
                traceVerifier.setExecutedStep(currentStep);
                traceVerifier.verify();
                if (currentStep > stepCount) line = "COMPLETE";
                statistics.reportCurrentStep("[LINE " + currentStep + "]-" + line);
                statistics.reportTotalExecutedEvents(totalExecuted);
                statisticsWriter.write(statistics.toString() + "\n\n");
                LOG.info(statistics.toString() + "\n\n\n\n\n");

                // shutdown clients & servers
                for (Integer i: clientMap.keySet()) {
                    LOG.debug("shutting down client {}", i);
                    clientMap.get(i).shutdown();
                }
                clientMap.clear();
                ensemble.stopEnsemble();

                executionWriter.close();
                statisticsWriter.close();
            }
        }
        LOG.debug("total time: {}" , (System.currentTimeMillis() - startTime));
        
    }

    public String getServerAddr(int serverId) {
        return "localhost:" + (schedulerConfiguration.getClientPort() + serverId);
    }

    public void initRemote() {
        try {
            final TestingRemoteService schedulerRemote = (TestingRemoteService) UnicastRemoteObject.exportObject(this, 0);
            final Registry registry = LocateRegistry.createRegistry(2599);
//            final Registry registry = LocateRegistry.getRegistry(2599);
            LOG.debug("{}, {}", TestingRemoteService.REMOTE_NAME, schedulerRemote);
            registry.rebind(TestingRemoteService.REMOTE_NAME, schedulerRemote);
            LOG.debug("Bound the remote scheduler");
        } catch (final RemoteException e) {
            LOG.error("Encountered a remote exception while initializing the scheduler.", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getMessageInFlight() {
        return messageInFlight;
    }

    @Override
    public int getLogRequestInFlight() {
        return logRequestInFlight;
    }

    public void setLastNodeStartEvent(final int nodeId, final NodeStartEvent nodeStartEvent) {
        lastNodeStartEvents.set(nodeId, nodeStartEvent);
    }

    /***
     * Configure scheduling strategy before each execution
     * @param executionId
     */
    private void configureSchedulingStrategy(final int executionId) {
        // Configure scheduling strategy
        final Random random = new Random();
        final long seed;
        if (schedulerConfiguration.hasRandomSeed()) {
            seed = schedulerConfiguration.getRandomSeed() + executionId * executionId;
        }
        else {
            seed = random.nextLong();
        }
        random.setSeed(seed);

        if ("pctcp".equals(schedulerConfiguration.getSchedulingStrategy())) {
            final PCTCPStatistics pctcpStatistics = new PCTCPStatistics();
            statistics = pctcpStatistics;

            LOG.debug("Configuring PCTCPStrategy: maxEvents={}, numPriorityChangePoints={}, randomSeed={}",
                    schedulerConfiguration.getMaxEvents(), schedulerConfiguration.getNumPriorityChangePoints(), seed);
            pctcpStatistics.reportMaxEvents(schedulerConfiguration.getMaxEvents());
            pctcpStatistics.reportNumPriorityChangePoints(schedulerConfiguration.getNumPriorityChangePoints());
            schedulingStrategy = new PCTCPStrategy(schedulerConfiguration.getMaxEvents(),
                    schedulerConfiguration.getNumPriorityChangePoints(), random, pctcpStatistics);
        }
        else if ("tapct".equals(schedulerConfiguration.getSchedulingStrategy())) {
            final TAPCTstatistics tapctStatistics = new TAPCTstatistics();
            statistics = tapctStatistics;

            LOG.debug("Configuring taPCTstrategy: maxEvents={}, numPriorityChangePoints={}, randomSeed={}",
                    schedulerConfiguration.getMaxEvents(), schedulerConfiguration.getNumPriorityChangePoints(), seed);
            tapctStatistics.reportMaxEvents(schedulerConfiguration.getMaxEvents());
            tapctStatistics.reportNumPriorityChangePoints(schedulerConfiguration.getNumPriorityChangePoints());
            schedulingStrategy = new TAPCTstrategy(
                    schedulerConfiguration.getMaxEvents(),
                    schedulerConfiguration.getNumPriorityChangePoints(), random, tapctStatistics);
        }
        else if ("Poffline".equals(schedulerConfiguration.getSchedulingStrategy())) {
            final PofflineStatistics pOfflineStatistics = new PofflineStatistics();
            statistics = pOfflineStatistics;

            LOG.debug("Configuring PofflineStrategy: maxEvents={}, numPriorityChangePoints={}, randomSeed={}",
                    schedulerConfiguration.getMaxEvents(), schedulerConfiguration.getNumPriorityChangePoints(), seed);
            pOfflineStatistics.reportMaxEvents(schedulerConfiguration.getMaxEvents());
            pOfflineStatistics.reportNumPriorityChangePoints(schedulerConfiguration.getNumPriorityChangePoints());
            schedulingStrategy = new PofflineStrategy(schedulerConfiguration.getMaxEvents(),
                    schedulerConfiguration.getNumPriorityChangePoints(), random, pOfflineStatistics);
        }
        else if("pos".equals(schedulerConfiguration.getSchedulingStrategy())) {
            final POSstatistics posStatistics = new POSstatistics();
            statistics = posStatistics;

            LOG.debug("Configuring POSstrategy: randomSeed={}", seed);
            schedulingStrategy = new POSstrategy(schedulerConfiguration.getMaxEvents(), random, posStatistics);
        }

        else if("posd".equals(schedulerConfiguration.getSchedulingStrategy())) {
            final POSdStatistics posdStatistics = new POSdStatistics();
            statistics = posdStatistics;

            posdStatistics.reportNumPriorityChangePoints(schedulerConfiguration.getNumPriorityChangePoints());
            LOG.debug("Configuring POSdStrategy: randomSeed={}", seed);
            schedulingStrategy = new POSdStrategy(schedulerConfiguration.getMaxEvents(), schedulerConfiguration.getNumPriorityChangePoints(), random, posdStatistics);
        }
        else if("rapos".equals(schedulerConfiguration.getSchedulingStrategy())) {
            final RAPOSstatistics raposStatistics = new RAPOSstatistics();
            statistics = raposStatistics;

            LOG.debug("Configuring RAPOSstrategy: randomSeed={}", seed);
            schedulingStrategy = new RAPOSstrategy(random, raposStatistics);
        }
        else {
            final RandomWalkStatistics randomWalkStatistics = new RandomWalkStatistics();
            statistics = randomWalkStatistics;

            LOG.debug("Configuring RandomWalkStrategy: randomSeed={}", seed);
            schedulingStrategy = new RandomWalkStrategy(random, randomWalkStatistics);
        }
        statistics.reportRandomSeed(seed);
    }

    /***
     * Configure all testing metadata before each execution
     */
    private void configureNextExecution() {

        final int nodeNum = schedulerConfiguration.getNumNodes();

        // Configure external event executors
        nodeStartExecutor = new NodeStartExecutor(this, schedulerConfiguration.getNumReboots());
        nodeCrashExecutor = new NodeCrashExecutor(this, schedulerConfiguration.getNumCrashes());

        clientRequestWaitingResponseExecutor = new ClientRequestExecutor(this, true, 0);
        clientRequestExecutor = new ClientRequestExecutor(this, false, 0);

        partitionStartExecutor = new PartitionStartExecutor(this, 10);
        partitionStopExecutor = new PartitionStopExecutor(this, 10);

        // Configure internal event executors
        electionMessageExecutor = new ElectionMessageExecutor(this);
        localEventExecutor = new LocalEventExecutor(this);
        leaderToFollowerMessageExecutor = new LeaderToFollowerMessageExecutor(this);
        followerToLeaderMessageExecutor = new FollowerToLeaderMessageExecutor(this);

        // Configure checkers
        traceVerifier = new TraceVerifier(this, statistics);
        leaderElectionVerifier = new LeaderElectionVerifier(this, statistics);
        getDataVerifier = new GetDataVerifier(this, statistics);
        committedLogVerifier = new CommittedLogVerifier(this, statistics);

        // for property check
        votes.clear();
        votes.addAll(Collections.<Vote>nCopies(nodeNum, null));

        leaderElectionStates.clear();
        leaderElectionStates.addAll(Collections.nCopies(nodeNum, LeaderElectionState.LOOKING));

        returnedDataList.clear();
        returnedDataList.add(0);

        lastCommittedZxid.clear();
        lastCommittedZxid.add(0L);

        lastProcessedZxids.clear();
        allZxidRecords.clear();

        zxidSyncedMap.clear();
        zxidToCommitMap.clear();
        leaderSyncFollowerCountMap.clear();

        syncTypeList.clear();

        traceMatched = true;
        tracePassed = true;

        // Configure nodes and subnodes

        nodeStates.clear();
        nodePhases.clear();

        subnodeSets.clear();
        subnodes.clear();
        nodeStateForClientRequests.clear();
        followerSocketAddressBook.clear();
        followerLearnerHandlerMap.clear();
        followerLearnerHandlerSenderMap.clear();

        participants.clear();

        // configure client map
        clientMap.clear();

        // configure network partion info
        partitionMap.clear();

//        partitionMap.addAll(Collections.nCopies(schedulerConfiguration.getNumNodes(),
//                new ArrayList<>(Collections.nCopies(schedulerConfiguration.getNumNodes(), false))));

        for (int i = 0 ; i < nodeNum; i++) {
            nodeStates.add(NodeState.STARTING);
            nodePhases.add(Phase.DISCOVERY);
            subnodeSets.add(new HashSet<Subnode>());
            lastProcessedZxids.add(0L);
            nodeStateForClientRequests.add(NodeStateForClientRequest.SET_DONE);
            followerSocketAddressBook.add(null);
            followerLearnerHandlerMap.add(null);
            followerLearnerHandlerSenderMap.add(null);

            syncTypeList.add(-1);

            partitionMap.add(new ArrayList<>(Collections.nCopies(nodeNum, false)));

            allZxidRecords.add(new ArrayList<>(Arrays.asList(0L)));
        }

        eventIdGenerator.set(0);
        clientIdGenerator.set(0);

        messageEventMap.clear();
        messageInFlight = 0;
        logRequestInFlight = 0;

        firstMessage.clear();
        firstMessage.addAll(Collections.<Boolean>nCopies(nodeNum, null));

        // Configure lastNodeStartEvents
        lastNodeStartEvents.clear();
        lastNodeStartEvents.addAll(Collections.<NodeStartEvent>nCopies(nodeNum, null));

        // Generate node crash events
        if (schedulerConfiguration.getNumCrashes() > 0) {
            for (int i = 0; i < nodeNum; i++) {
                final NodeCrashEvent nodeCrashEvent = new NodeCrashEvent(generateEventId(), i, nodeCrashExecutor);
                schedulingStrategy.add(nodeCrashEvent);
            }
        }
    }

    /***
     * Configure all testing metadata after election
     */
    private void configureAfterElection(String serverList) {
        // Initialize zkClients
        createClient(1, true, serverList);

        // Reconfigure executors
        nodeStartExecutor = new NodeStartExecutor(this, schedulerConfiguration.getNumRebootsAfterElection());
        nodeCrashExecutor = new NodeCrashExecutor(this, schedulerConfiguration.getNumCrashesAfterElection());
    }

    /***
     * The schedule process of election and discovery
     * Pre-condition: all alive participants steady and in looking state including the leader
     * Post-condition: all nodes voted & all nodes ready for sync
     * Property check: one leader has been elected
     * @param leaderId the leader that model specifies
     * @param totalExecuted the number of previous executed events
     * @return the number of executed events
     */
    public int scheduleElectionAndDiscovery(final Integer currentStep,
                                            final Integer leaderId,
                                            final List<Integer> peers,
                                            int totalExecuted) throws SchedulerConfigurationException {
        try{
//            statistics.startTimer();
            Set<Integer> lookingParticipants = new HashSet<>(peers);
            lookingParticipants.add(leaderId);
            synchronized (controlMonitor) {
                // pre-condition
                waitAliveNodesInLookingState(lookingParticipants);

                // Make all message events during election one single step
                ++totalExecuted;
                boolean leaderExists = false;
                int retry = 0;
                Set<Event> nonElectionEvents = new HashSet<>();
                // TODO: how to terminate?
                while (!leaderExists && retry < 5) {
                    retry++;
                    while (schedulingStrategy.hasNextEvent() && totalExecuted < 100) {
                        long begintime = System.currentTimeMillis();
                        LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted);
                        final Event event = schedulingStrategy.nextEvent();
//                        // TODO: this may wrongly delete newly produced LeaderToFollowerMessageEvent!
//                        // TODO: should be make executed when the node crash
//                        if (event instanceof LeaderToFollowerMessageEvent) {
//                            final int sendingSubnodeId = ((LeaderToFollowerMessageEvent) event).getSendingSubnodeId();
//
//                            final int receivingNodeId = ((LeaderToFollowerMessageEvent) event).getReceivingNodeId();
//                            if (peers.contains(receivingNodeId)) {
//                                // confirm this works / use partition / let
//                                deregisterSubnode(sendingSubnodeId);
//                                ((LeaderToFollowerMessageEvent) event).setExecuted();
//                                LOG.debug("----Do not let the previous learner handler message occur here! So pass this event---------\n\n\n");
//                                continue;
//                            }
//                        }
                        if (!(event instanceof ElectionMessageEvent)) {
                            nonElectionEvents.add(event);
                            continue;
                        }
                        if (event.execute()) {
                            recordProperties(totalExecuted, begintime, event);
                        }
//                        if (!event.execute()) {
//                            LOG.debug("something wrong during election when executing {}", event);
//                        }
                    }
                    // pre-condition for election property check
                    leaderExists = waitAllParticipantsVoted(lookingParticipants);
                }
                if (!leaderExists) {
                    LOG.debug("Leader not exist! " +
                            "SchedulerConfigurationException found when scheduling ElectionAndDiscovery." +
                            " leader: " + leaderId + " peers: " + peers);
                    throw new SchedulerConfigurationException();
                }
                participants.addAll(lookingParticipants);
                for (Event e: nonElectionEvents) {
                    LOG.debug("Adding back event that is missed during election: {}", e);
                    addEvent(e);
                }

                // wait for the event LeaderSyncFollower?
                leaderSyncFollowerCountMap.put(leaderId, peers.size());
                waitLeaderSyncReady(leaderId, peers);
            }
            statistics.endTimer();
            // check election results
            leaderElectionVerifier.setModelResult(leaderId);
            leaderElectionVerifier.setParticipants(lookingParticipants);
            leaderElectionVerifier.verify();
            // report statistics
            if (currentStep != null && peers != null) {
                statistics.reportCurrentStep("[Step " + currentStep + "]-"
                        + "ElectionAndDiscovery, leader: " + leaderId
                        + " peers: " + peers);
            }
            statistics.reportTotalExecutedEvents(totalExecuted);
            statisticsWriter.write(statistics.toString() + "\n\n");
            LOG.info(statistics.toString() + "\n\n\n\n\n");
            if (!traceMatched) {
                LOG.debug("UNMATCH model during ElectionAndDiscovery. " +
                        " leader: " + leaderId + " peers: " + peers);
                throw new SchedulerConfigurationException();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }


    /***
     * The schedule process of leader election
     * Pre-condition: all nodes steady
     * Post-condition: all nodes voted & all nodes steady before request
     * Property check: one leader has been elected
     * @param leaderId the leader that model specifies
     * @param totalExecuted the number of previous executed events
     * @return the number of executed events
     */
    // TODO: add partition events during election
    public int scheduleElection(final Integer currentStep, final String line, Integer leaderId, int totalExecuted) {
        try{
//            statistics.startTimer();
            synchronized (controlMonitor) {
                // pre-condition
//                waitAllNodesSteady();
                waitAliveNodesInLookingState();
                while (schedulingStrategy.hasNextEvent() && totalExecuted < 100) {
                    long begintime = System.currentTimeMillis();
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                    final Event event = schedulingStrategy.nextEvent();
                    if (event instanceof LeaderToFollowerMessageEvent) {
                        final int sendingSubnodeId = ((LeaderToFollowerMessageEvent) event).getSendingSubnodeId();
                        // confirm this works / use partition / let
                        deregisterSubnode(sendingSubnodeId);
                        ((LeaderToFollowerMessageEvent) event).setExecuted();
                        LOG.debug("----Do not let the previous learner handler message occur here! So pass this event---------\n\n\n");
                        continue;
                    }
                    if (event.execute()) {
                        ++totalExecuted;
                        recordProperties(totalExecuted, begintime, event);
                    }
                }
                // pre-condition for election property check
                waitAllNodesVoted();
//                waitAllNodesSteadyBeforeRequest();
            }
            statistics.endTimer();
            // check election results
            leaderElectionVerifier.setModelResult(leaderId);
            leaderElectionVerifier.verify();
            // report statistics
            if (currentStep != null && line != null) {
                statistics.reportCurrentStep("[LINE " + currentStep + "]-" + line);
            }
            statistics.reportTotalExecutedEvents(totalExecuted);
            statisticsWriter.write(statistics.toString() + "\n\n");
            LOG.info(statistics.toString() + "\n\n\n\n\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    /***
     * The schedule process after the first round of election
     * @param totalExecuted the number of previous executed events
     * @return the number of executed events
     */
    private int scheduleAfterElection(int totalExecuted) {
        try {
            synchronized (controlMonitor) {
                waitAllNodesSteady();
                LOG.debug("All Nodes steady");
                while (schedulingStrategy.hasNextEvent() && totalExecuted < 100) {
                    long begintime = System.currentTimeMillis();
                    for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); ++nodeId) {
                        LOG.debug("--------------->Node Id: {}, NodeState: {}, " +
                                        "role: {}, " +
                                        "vote: {}",nodeId, nodeStates.get(nodeId),
                                leaderElectionStates.get(nodeId),
                                votes.get(nodeId)
                        );
                    }
                    executionWriter.write("\n\n---Step: " + totalExecuted + "--->");
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted);
                    final Event event = schedulingStrategy.nextEvent();
                    // Only the leader will be crashed
                    if (event instanceof NodeCrashEvent){
                        LeaderElectionState RoleOfCrashNode = leaderElectionStates.get(((NodeCrashEvent) event).getNodeId());
                        LOG.debug("----role: {}---------", RoleOfCrashNode);
                        if ( RoleOfCrashNode != LeaderElectionState.LEADING){
                            ((NodeCrashEvent) event).setExecuted();
                            LOG.debug("----pass this event---------\n\n\n");
                            continue;
                        }
                        if (event.execute()) {
                            ++totalExecuted;

                            // wait for new message from online nodes after the leader crashed
                            waitNewMessageOffered();

                            long endtime=System.currentTimeMillis();
                            long costTime = (endtime - begintime);
                            executionWriter.write("-------waitNewMessageOffered cost_time: " + costTime + "\n");
                        }
                    }
                    else if (event.execute()) {
                        ++totalExecuted;
                        long endtime=System.currentTimeMillis();
                        long costTime = (endtime - begintime);
                        executionWriter.write("------cost_time: " + costTime + "\n");
                    }
                }
                waitAllNodesVoted();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    /***
     * The schedule process after the first round of election with client requests
     * @param totalExecuted the number of previous executed events
     * @return the number of executed events
     */
    private int scheduleClientRequests(int totalExecuted) {
        try {
            synchronized (controlMonitor) {
                waitAllNodesSteadyBeforeRequest();
                LOG.debug("All Nodes steady for client requests");
                for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); nodeId++) {
                    executionWriter.write(lastProcessedZxids.get(nodeId).toString() + " # ");
                }
                for (int i = 1; i <= schedulerConfiguration.getNumClientRequests() ; i++) {
                    long begintime = System.currentTimeMillis();
                    executionWriter.write("\n\n---Step: " + totalExecuted + "--->\n");
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted);
                    LOG.debug("Client request: {}", i);
                    final Event event = schedulingStrategy.nextEvent();
                    LOG.debug("prepare to execute event: {}", event.toString());
                    if (event.execute()) {
                        ++totalExecuted;
                        LOG.debug("executed event: {}", event.toString());
                        long endtime=System.currentTimeMillis();
                        long costTime = (endtime - begintime);
                        executionWriter.write("-----cost_time: " + costTime + "\n");
                        // Property verification
                        for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); nodeId++) {
                            executionWriter.write(lastProcessedZxids.get(nodeId).toString() + " # ");
                        }
                    }
                }
//                executionWriter.write("---Step: " + totalExecuted + "--->");
//                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted);
//                LOG.debug("Client request: set data");
//                final Event event = schedulingStrategy.nextEvent();
//                LOG.debug("prepare to execute event: {}", event.toString());
//                if (event.execute()) {
//                    ++totalExecuted;
//                    LOG.debug("executed event: {}", event.toString());
//                }

//                nodeStartExecutor = new NodeStartExecutor(this, executionWriter, 1);
//                nodeCrashExecutor = new NodeCrashExecutor(this, executionWriter, 1);
//                final NodeCrashEvent nodeCrashEvent0 = new NodeCrashEvent(generateEventId(), 0, nodeCrashExecutor);
//                schedulingStrategy.add(nodeCrashEvent0);
//                final NodeCrashEvent nodeCrashEvent1 = new NodeCrashEvent(generateEventId(), 1, nodeCrashExecutor);
//                schedulingStrategy.add(nodeCrashEvent1);
//
//                executionWriter.write("---Step: " + totalExecuted + "--->");
//                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted);
//                LOG.debug("prepare to execute event: {}", nodeCrashEvent0.toString());
//                if (nodeCrashEvent0.execute()) {
//                    ++totalExecuted;
//                    LOG.debug("executed event: {}", nodeCrashEvent0.toString());
//                }
//
//                executionWriter.write("---Step: " + totalExecuted + "--->");
//                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted);
//                LOG.debug("prepare to execute event: {}", nodeCrashEvent1.toString());
//                if (nodeCrashEvent1.execute()) {
//                    ++totalExecuted;
//                    LOG.debug("executed event: {}", nodeCrashEvent1.toString());
//                }

                waitAllNodesVoted();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    /***
     * create client session
     * Note: when client is initializing, servers are better not allowed to be intercepted
     *      o.w. the initialization may be blocked and fail.
     */
    private void createClient(final int clientId, final boolean resetConnectionState, final String serverList) {
        if (resetConnectionState) {
            clientInitializationDone = false;
        }
        ClientProxy clientProxy = new ClientProxy(this, clientId, serverList);
        clientMap.put(clientId, clientProxy);
        LOG.debug("------------------start the client session initialization------------------");

        clientProxy.start();
        synchronized (controlMonitor) {
            controlMonitor.notifyAll();
            waitClientSessionReady(clientId);
        }
        clientInitializationDone = true;

        // wait for specific time
//        while (!clientProxy.isReady()) {
//            LOG.debug("------------------still initializing the client session------------------");
//            try {
//                Thread.sleep(100);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
        LOG.debug("------------------finish the client session initialization------------------\n");
    }

    /***
     * create client session
     * Note: when client is initializing, servers are better not allowed to be intercepted
     *      o.w. the initialization may be blocked and fail.
     */
    private void establishSession(final int clientId,
                                  final boolean resetConnectionState,
                                  final String serverList) {
        synchronized (controlMonitor) {
            // pre-condition
            waitAllNodesSteadyBeforeRequest();
            if (resetConnectionState) {
                clientInitializationDone = false;
            }
            ClientProxy clientProxy = new ClientProxy(this, clientId, serverList);
            clientMap.put(clientId, clientProxy);

            LOG.debug("------------------start the client session initialization------------------");
            clientProxy.start();
            controlMonitor.notifyAll();
            // post-condition
            waitClientSessionReady(clientId);

            clientInitializationDone = true;

            LOG.debug("------------------finish the client session initialization------------------\n");
        }
    }

    /***
     * This triggers two events :
     *  --> setData
     *  --> Leader log proposal
     *  --> release LeaderToFollowerMessageEvent(PROPOSAL) if partition un-exists
     *  Note: if without specific data, will use eventId as its written string value
     */
    private int scheduleLeaderProcessRequest(ExternalModelStrategy strategy,
                                             final int clientId,
                                             final int serverId,
                                             final String data,
                                             int totalExecuted) throws SchedulerConfigurationException {
        try {
            // Step 0. establish session if un-exists
            ClientProxy clientProxy = clientMap.get(clientId);
            if (clientProxy == null || clientProxy.isStop()) {
                String serverAddr = getServerAddr(serverId);
                LOG.debug("client establish connection with server {}", serverAddr);
                establishSession(clientId, true, serverAddr);
            }

            // Step 1. setData
            synchronized (controlMonitor) {
                waitAllNodesSteadyBeforeRequest();
                long startTime = System.currentTimeMillis();
                Event event;
                if (data == null) {
                    event = new ClientRequestEvent(generateEventId(), clientId,
                            ClientRequestType.SET_DATA, clientRequestExecutor);
                } else {
                    event = new ClientRequestEvent(generateEventId(), clientId,
                            ClientRequestType.SET_DATA, data, clientRequestExecutor);
                }
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
//                    ++totalExecuted;
                    recordProperties(totalExecuted + 1, startTime, event);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Step 2. Leader log proposal
        totalExecuted = scheduleInternalEventWithWaitingRetry(strategy, ModelAction.LogPROPOSAL, serverId, -1, totalExecuted, 1);

        // Step 3. release LeaderToFollowerPROPOSAL(PROPOSAL) if partition un-exists
        totalExecuted = scheduleLeaderToFollowerPROPOSAL(strategy, totalExecuted);

        return totalExecuted;
    }

    private int scheduleLeaderToFollowerPROPOSAL(ExternalModelStrategy strategy,
                                                 int totalExecuted) throws SchedulerConfigurationException {
        try {
            synchronized (controlMonitor) {
                Set<Event> otherEvents = new HashSet<>();
                while (schedulingStrategy.hasNextEvent() && totalExecuted < 100) {
                    long begintime = System.currentTimeMillis();
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted);
                    final Event event = schedulingStrategy.nextEvent();
                    if (! (event instanceof LeaderToFollowerMessageEvent)) {
                        otherEvents.add(event);
                        continue;
                    }
                    LeaderToFollowerMessageEvent e = (LeaderToFollowerMessageEvent) event;
                    if (MessageType.PROPOSAL != e.getType()) {
                        otherEvents.add(event);
                        continue;
                    }
                    if (event.execute()) {
                        recordProperties(totalExecuted, begintime, event);
                    }
                }
                for (Event e: otherEvents) {
                    LOG.debug("Adding back event that is missed during LeaderToFollowerPROPOSAL: {}", e);
                    addEvent(e);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    private int scheduleLeaderProcessACK(ExternalModelStrategy strategy,
                                             final int leaderId,
                                             final int followerId,
                                             int totalExecuted) throws SchedulerConfigurationException {

        // Step 1. check leader's local commit is done
        try {
            LOG.debug("try to schedule LeaderProcessCOMMIT leaderId: {}", leaderId);
            scheduleInternalEvent(strategy, ModelAction.LeaderProcessCOMMIT, leaderId, -1, totalExecuted);
        } catch (SchedulerConfigurationException e2) {
            LOG.debug("SchedulerConfigurationException found when scheduling leader's local commit! This will be fine.");
        }
        LOG.debug("try to schedule LeaderToFollowerCOMMIT leaderId: {}, followerId: {}", leaderId, followerId);
        totalExecuted = scheduleInternalEvent(strategy, ModelAction.LeaderToFollowerCOMMIT, leaderId, followerId, totalExecuted);

        return totalExecuted;
    }

    /***
     * setData with specific data
     * if without specific data, will use eventId as its written string value
     */
    private int scheduleSetData(final int clientId,
                                final int serverId,
                                final String data,
                                int totalExecuted) {
        try {
            ClientProxy clientProxy = clientMap.get(clientId);
            if (clientProxy == null || clientProxy.isStop()) {
                String serverAddr = getServerAddr(serverId);
                LOG.debug("client establish connection with server {}", serverAddr);
                establishSession(clientId, true, serverAddr);
            }
            synchronized (controlMonitor) {
                waitAllNodesSteadyBeforeRequest();
                long startTime = System.currentTimeMillis();
                Event event;
                if (data == null) {
                    event = new ClientRequestEvent(generateEventId(), clientId,
                            ClientRequestType.SET_DATA, clientRequestExecutor);
                } else {
                    event = new ClientRequestEvent(generateEventId(), clientId,
                            ClientRequestType.SET_DATA, data, clientRequestExecutor);
                }
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    /***
     * getData for externalModelStrategy
     */
    private int scheduleClientGetData(final Integer currentStep,
                                      final int clientId,
                                      final int serverId,
                                      final Integer modelResult,
                                      int totalExecuted) {
        try {
            ClientProxy clientProxy = clientMap.get(clientId);
            if (clientProxy == null || clientProxy.isStop())  {
                String serverAddr = getServerAddr(serverId);
                LOG.debug("client establish connection with server {}", serverAddr);
                establishSession(clientId, true, serverAddr);
            }
            synchronized (controlMonitor) {
                waitAllNodesSteadyBeforeRequest();
                long startTime = System.currentTimeMillis();
                Event event = new ClientRequestEvent(generateEventId(), clientId,
                        ClientRequestType.GET_DATA, clientRequestWaitingResponseExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
            statistics.endTimer();
            // check election results
            getDataVerifier.setModelResult(modelResult);
            getDataVerifier.verify();
            // report statistics
            if (currentStep != null ) {
                statistics.reportCurrentStep("[Step " + currentStep + "]-ClientGetData");
            }
            statistics.reportTotalExecutedEvents(totalExecuted);
            statisticsWriter.write(statistics.toString() + "\n\n");
            LOG.info(statistics.toString() + "\n\n\n\n\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    /***
     * getData
     */
    private int scheduleGetData(final Integer currentStep,
                                final String line,
                                final int clientId,
                                final int serverId,
                                final Integer modelResult,
                                int totalExecuted) {
        try {
            ClientProxy clientProxy = clientMap.get(clientId);
            if (clientProxy == null || clientProxy.isStop())  {
                String serverAddr = getServerAddr(serverId);
                LOG.debug("client establish connection with server {}", serverAddr);
                establishSession(clientId, true, serverAddr);
            }
            synchronized (controlMonitor) {
                waitAllNodesSteadyBeforeRequest();
                long startTime = System.currentTimeMillis();
                Event event = new ClientRequestEvent(generateEventId(), clientId,
                        ClientRequestType.GET_DATA, clientRequestWaitingResponseExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
            statistics.endTimer();
            // check election results
            getDataVerifier.setModelResult(modelResult);
            getDataVerifier.verify();
            // report statistics
            if (currentStep != null && line != null) {
                statistics.reportCurrentStep("[LINE " + currentStep + "]-" + line);
            }
            statistics.reportTotalExecutedEvents(totalExecuted);
            statisticsWriter.write(statistics.toString() + "\n\n");
            LOG.info(statistics.toString() + "\n\n\n\n\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    public int scheduleInternalEventWithRetry(ExternalModelStrategy strategy,
                                     final ModelAction action,
                                     final int nodeId,
                                     final int peerId,
                                     int totalExecuted, int retry) throws SchedulerConfigurationException {
        while (true) {
            try {
                synchronized (controlMonitor) {
                    long startTime = System.currentTimeMillis();
                    Event event = strategy.getNextInternalEvent(action, nodeId, peerId);
                    assert event != null;
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                    LOG.debug("prepare to execute event: {}", event);
                    if (event.execute()) {
                        ++totalExecuted;
                        recordProperties(totalExecuted, startTime, event);
                    }
                }
                break;
            } catch (IOException e) {
                e.printStackTrace();
                break;
            } catch (SchedulerConfigurationException e2) {
                LOG.debug("SchedulerConfigurationException found when scheduling {}! Retry: {}", action, retry);
                retry--;
                if (retry <= 0) {
                    throw e2;
                }
            }
        }
        return totalExecuted;
    }

    public int scheduleInternalEventWithWaitingRetry(ExternalModelStrategy strategy,
                                              final ModelAction action,
                                              final int nodeId,
                                              final int peerId,
                                              int totalExecuted, int retry) throws SchedulerConfigurationException {
        while (retry > 0) {
            try {
                synchronized (controlMonitor) {
                    long startTime = System.currentTimeMillis();
                    Event event = waitTargetInternalEventReady(strategy, action, nodeId, peerId);
                    if (event == null) {
                        retry--;
                        LOG.debug("target internal event not found! will wait with retry {} more time(s).", retry);
                        continue;
                    }
//                    Event event = strategy.getNextInternalEvent(action, nodeId, peerId);
//                    assert event != null;
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                    LOG.debug("prepare to execute event: {}", event);
                    if (event.execute()) {
                        ++totalExecuted;
                        recordProperties(totalExecuted, startTime, event);
                    }
                }
                break;
            } catch (IOException e) {
                e.printStackTrace();
                break;
            }
//            catch (SchedulerConfigurationException e2) {
//                LOG.debug("SchedulerConfigurationException found when scheduling {}! Retry: {}", action, retry);
//                retry--;
//                if (retry <= 0) {
//                    throw e2;
//                }
//            }
        }
        if (retry <= 0) {
            LOG.debug("SchedulerConfigurationException found when scheduling {}! Retry: {}", action, retry);
            throw new SchedulerConfigurationException();
        }
        return totalExecuted;
    }

    public int scheduleInternalEvent(ExternalModelStrategy strategy,
                                     final ModelAction action,
                                     final int nodeId,
                                     final int peerId,
                                     int totalExecuted) throws SchedulerConfigurationException {
        try {
            synchronized (controlMonitor) {
//                    waitTargetInternalEventReady(strategy, action, nodeId, peerId);
                long startTime = System.currentTimeMillis();
                Event event = strategy.getNextInternalEvent(action, nodeId, peerId);
                assert event != null;
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SchedulerConfigurationException e2) {
            LOG.debug("SchedulerConfigurationException found when scheduling {}!", action);
            throw e2;
        }
        return totalExecuted;
    }

    public int scheduleInternalEventInSequence(EventSequenceStrategy strategy,
                                               String[] lineArr,
                                               int totalExecuted) throws SchedulerConfigurationException {
        try {
            synchronized (controlMonitor) {
                long startTime = System.currentTimeMillis();
                Event event = strategy.getNextInternalEvent(lineArr);
                assert event != null;
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SchedulerConfigurationException e2) {
            LOG.debug("SchedulerConfigurationException found when scheduling {}!", Arrays.toString(lineArr));
            throw e2;
        }
        return totalExecuted;
    }


    /***
     * log request
     */
    private int scheduleLogRequest(final int nodeId, int totalExecuted) {
        try {
            synchronized (controlMonitor) {
                long startTime = System.currentTimeMillis();
                Event event = schedulingStrategy.nextEvent();
                // may be trapped in loop
                while (!(event instanceof LocalEvent) ||
                        !(SubnodeType.SYNC_PROCESSOR.equals(((LocalEvent) event).getSubnodeType())) ||
                        ((LocalEvent) event).getNodeId() != nodeId){
                    LOG.debug("-------need node {} 's SyncRequestEvent! get event: {}", nodeId, event);
                    addEvent(event);
                    event = schedulingStrategy.nextEvent();
                }
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    /***
     * commit
     */
    private int scheduleCommit(final int nodeId, int totalExecuted) {
        try {
            synchronized (controlMonitor) {
                long startTime = System.currentTimeMillis();
                Event event = schedulingStrategy.nextEvent();
                // this may be trapped in loop
                while (!(event instanceof LocalEvent) ||
                        !(SubnodeType.COMMIT_PROCESSOR.equals(((LocalEvent) event).getSubnodeType())) ||
                        ((LocalEvent) event).getNodeId() != nodeId){
                    LOG.debug("-------need node {} 's CommitEvent! get event: {}", nodeId, event);
                    addEvent(event);
                    event = schedulingStrategy.nextEvent();
                }
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    /***
     * schedule learner handler event
     */
    private int scheduleLearnerHandlerMessage(final int sendingNode, final int receivingNode, int totalExecuted) {
        try {
            synchronized (controlMonitor) {
                long startTime = System.currentTimeMillis();
                Event event = schedulingStrategy.nextEvent();
                // this may be trapped in loop
                while (!(event instanceof LeaderToFollowerMessageEvent) ||
                        ((LeaderToFollowerMessageEvent) event).getReceivingNodeId() != receivingNode){
                    LOG.debug("-------need node {} 's LeaderToFollowerMessageEvent to node {}! get event: {}",
                            sendingNode, receivingNode, event);
                    addEvent(event);
                    event = schedulingStrategy.nextEvent();
                }
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    /***
     * Node crash
     */
    private int scheduleNodeCrash(int nodeId, int totalExecuted) {
        try {
            assert NodeState.ONLINE.equals(nodeStates.get(nodeId));
            // TODO: move this to configuration file
            if (!nodeCrashExecutor.hasCrashes()) {
                nodeCrashExecutor = new NodeCrashExecutor(this, 1);
            }
            synchronized (controlMonitor) {
                long startTime = System.currentTimeMillis();
                Event event = new NodeCrashEvent(generateEventId(), nodeId, nodeCrashExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    /***
     * Node start
     */
    private int scheduleNodeStart(int nodeId, int totalExecuted) {
        try {
            assert NodeState.OFFLINE.equals(nodeStates.get(nodeId));
            if (!nodeStartExecutor.hasReboots()) {
                nodeStartExecutor = new NodeStartExecutor(this, 1);
            }
            synchronized (controlMonitor) {
                long startTime = System.currentTimeMillis();
                Event event = new NodeStartEvent(generateEventId(), nodeId, nodeStartExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }


    private int schedulePartitionStart(int node1, int node2, int totalExecuted) {
        try {
            assert !partitionMap.get(node1).get(node2);
            assert !partitionMap.get(node2).get(node1);
            if (!partitionStartExecutor.enablePartition()) {
                partitionStartExecutor = new PartitionStartExecutor(this, 1);
            }
            synchronized (controlMonitor) {
                long startTime = System.currentTimeMillis();
                Event event = new PartitionStartEvent(generateEventId(), node1, node2, partitionStartExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    private int schedulePartitionStop(int node1, int node2, int totalExecuted) {
        try {
            assert partitionMap.get(node1).get(node2);
            assert partitionMap.get(node2).get(node1);
            if (!partitionStopExecutor.enablePartitionStop()) {
                partitionStopExecutor = new PartitionStopExecutor(this, 1);
            }
            synchronized (controlMonitor) {
                long startTime = System.currentTimeMillis();
                Event event = new PartitionStopEvent(generateEventId(), node1, node2, partitionStopExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    /***
     * steps to trigger DIFF
     * @param totalExecuted the first step sequence number
     * @return the last step sequence number
     */
    private int triggerDiffByLeaderRestart(int totalExecuted, final int clientId) {
        try {
            long startTime;
            Event event;
            synchronized (controlMonitor) {
                // PRE
                // >> client get request
                startTime = System.currentTimeMillis();
                event = new ClientRequestEvent(generateEventId(), clientId,
                        ClientRequestType.GET_DATA, clientRequestExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }

                // >> client set request
                startTime = System.currentTimeMillis();
                ClientRequestEvent mutationEvent = new ClientRequestEvent(generateEventId(), clientId,
                        ClientRequestType.SET_DATA, clientRequestExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", mutationEvent);
                if (mutationEvent.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, mutationEvent);
                }

                while (schedulingStrategy.hasNextEvent() && totalExecuted < 100) {
                    startTime = System.currentTimeMillis();
                    event = schedulingStrategy.nextEvent();
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                    LOG.debug("prepare to execute event: {}", event.toString());
                    if (event.execute()) {
                        ++totalExecuted;
                        recordProperties(totalExecuted, startTime, event);
                    }
                }

//                // >> client get request
//                startTime = System.currentTimeMillis();
//                event = new ClientRequestEvent(generateEventId(), clientId,
//                        ClientRequestType.GET_DATA, clientRequestExecutor);
//                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
//                LOG.debug("prepare to execute event: {}", event);
//                if (event.execute()) {
//                    ++totalExecuted;
//                    recordProperties(totalExecuted, startTime, event);
//                }

                // TODO: check quorum nodes has updated lastProcessedZxid whenever a client mutation is done
//                waitQuorumZxidUpdated();

                // in case the previous COMMIT REQUEST EVENT blocked the later execution
                waitResponseForClientRequest(mutationEvent);

                // make DIFF: client set >> leader log >> leader restart >> re-election
                // Step 1. client request SET_DATA
                startTime = System.currentTimeMillis();
                event = new ClientRequestEvent(generateEventId(), clientId,
                        ClientRequestType.SET_DATA, clientRequestExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }

                // Step 2. leader log
                startTime = System.currentTimeMillis();
                event = schedulingStrategy.nextEvent();
                while (!(event instanceof LocalEvent)){
                    LOG.debug("-------need SyncRequestEvent! get event: {}", event);
                    addEvent(event);
                    event = schedulingStrategy.nextEvent();
                }
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }

                // Step 3. Leader crash
                startTime = System.currentTimeMillis();
                // find leader
                int leader = -1;
                for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); ++nodeId) {
                    if (LeaderElectionState.LEADING.equals(leaderElectionStates.get(nodeId))) {
                        leader = nodeId;
                        LOG.debug("-----current leader: {}", leader);
                        break;
                    }
                }
                event = new NodeCrashEvent(generateEventId(), leader, nodeCrashExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }

                // Step 4. Leader restart
                startTime = System.currentTimeMillis();
                event = schedulingStrategy.nextEvent();
                while (true) {
                    if (event instanceof NodeStartEvent) {
                        final int nodeId = ((NodeStartEvent) event).getNodeId();
                        LeaderElectionState RoleOfCrashNode = leaderElectionStates.get(nodeId);
                        LOG.debug("----previous role of this crashed node {}: {}---------", nodeId, RoleOfCrashNode);
                        if (nodeId == leader){
                            break;
                        }
                    }
                    LOG.debug("-------need NodeStartEvent of the previous crashed leader! add this event back: {}", event);
                    addEvent(event);
                    event = schedulingStrategy.nextEvent();
                }
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }

                // Step 5. re-election and becomes leader
                // >> This part is also for random test
                while (schedulingStrategy.hasNextEvent() && totalExecuted < 100) {
                    startTime = System.currentTimeMillis();
                    event = schedulingStrategy.nextEvent();
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                    LOG.debug("prepare to execute event: {}", event.toString());
                    if (event.execute()) {
                        ++totalExecuted;
                        recordProperties(totalExecuted, startTime, event);
                    }
                }
                // wait whenever an election ends
                waitAllNodesVoted();
//                leaderElectionVerifier.verify();
                waitAllNodesSteadyBeforeRequest();

            }
            statistics.endTimer();
            // check election results
            leaderElectionVerifier.verify();
            statistics.reportTotalExecutedEvents(totalExecuted);
            statisticsWriter.write(statistics.toString() + "\n\n");
            LOG.info(statistics.toString() + "\n\n\n\n\n");

        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    private int triggerDiffByFollowersRestart(int totalExecuted, final int clientId) {
        try {
            long startTime;
            Event event;
            synchronized (controlMonitor) {
                // make DIFF: client set >> leader log >> leader restart >> re-election
                // Step 1. client request SET_DATA
                startTime = System.currentTimeMillis();
                event = new ClientRequestEvent(generateEventId(), clientId,
                        ClientRequestType.SET_DATA, clientRequestExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }

                // Step 2. leader log
                startTime = System.currentTimeMillis();
                event = schedulingStrategy.nextEvent();
                while (!(event instanceof LocalEvent)){
                    LOG.debug("-------need SyncRequestEvent! get event: {}", event);
                    addEvent(event);
                    event = schedulingStrategy.nextEvent();
                }
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }

                // follower crash and restart first, then leader crash
                // distinguish leader and followers
                int leader = -1;
                List<Integer> followersId = new ArrayList<>();
                for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); ++nodeId) {
                    switch (leaderElectionStates.get(nodeId)) {
                        case LEADING:
                            leader = nodeId;
                            LOG.debug("-----current leader: {}", nodeId);
                            break;
                        case FOLLOWING:
                            LOG.debug("-----find a follower: {}", nodeId);
                            followersId.add(nodeId);
                    }
                }

                // Step 3. each follower crash and restart
                for (int followerId: followersId) {

                    // follower crash
                    startTime = System.currentTimeMillis();
                    event = new NodeCrashEvent(generateEventId(), followerId, nodeCrashExecutor);
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                    LOG.debug("prepare to execute event: {}", event);
                    if (event.execute()) {
                        ++totalExecuted;
                        recordProperties(totalExecuted, startTime, event);
                    }

                    // follower restart
                    startTime = System.currentTimeMillis();
                    event = schedulingStrategy.nextEvent();
                    while (true) {
                        if (event instanceof NodeStartEvent) {
                            final int nodeId = ((NodeStartEvent) event).getNodeId();
                            LeaderElectionState RoleOfCrashNode = leaderElectionStates.get(nodeId);
                            LOG.debug("----previous role of this crashed node {}: {}---------", nodeId, RoleOfCrashNode);
                            if (nodeId == followerId) {
                                break;
                            }
                        }
                        LOG.debug("-------need NodeStartEvent of the previous crashed follower! add this event back: {}", event);
                        addEvent(event);
                        event = schedulingStrategy.nextEvent();
                    }
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                    LOG.debug("prepare to execute event: {}", event);
                    if (event.execute()) {
                        ++totalExecuted;
                        recordProperties(totalExecuted, startTime, event);
                    }

//                    //restart follower: LOOKING -> FOLLOWING
//                    while (schedulingStrategy.hasNextEvent() && totalExecuted < 100) {
//                        startTime = System.currentTimeMillis();
//                        event = schedulingStrategy.nextEvent();
//                        LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
//                        LOG.debug("prepare to execute event: {}", event.toString());
//                        if (event instanceof LeaderToFollowerMessageEvent) {
//                            final int sendingSubnodeId = ((LeaderToFollowerMessageEvent) event).getSendingSubnodeId();
//                            // confirm this works / use partition / let
//                            deregisterSubnode(sendingSubnodeId);
//                            ((LeaderToFollowerMessageEvent) event).setExecuted();
//
////                        schedulingStrategy.remove(event);
//                            LOG.debug("----Do not let the previous learner handler message occur here! So pass this event---------\n\n\n");
//                            continue;
//                        }
//                        else if (!(event instanceof ElectionMessageEvent)) {
//                            LOG.debug("not message event: {}, " +
//                                    "which means this follower begins to sync", event.toString());
//                            addEvent(event);
//                            if (leaderElectionStates.get(followerId).equals(LeaderElectionState.FOLLOWING)){
//                                break;
//                            }
//                        }
//                        else if (event.execute()) {
//                            ++totalExecuted;
//                            recordProperties(totalExecuted, startTime, event);
//                        }
//                    }
                }

                // wait Leader becomes LOOKING state
                waitAliveNodesInLookingState();

                // Step 4. re-election and becomes leader again
                // >> This part allows random schedulers
                while (schedulingStrategy.hasNextEvent() && totalExecuted < 100) {
                    startTime = System.currentTimeMillis();
                    event = schedulingStrategy.nextEvent();
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                    LOG.debug("prepare to execute event: {}", event.toString());
                    if (event instanceof LeaderToFollowerMessageEvent) {
                        final int sendingSubnodeId = ((LeaderToFollowerMessageEvent) event).getSendingSubnodeId();
                        // confirm this works / use partition / let
                        deregisterSubnode(sendingSubnodeId);
                        ((LeaderToFollowerMessageEvent) event).setExecuted();
                        LOG.debug("----Do not let the previous learner handler message occur here! So pass this event---------\n\n\n");
                        continue;
                    }
//                    else if (event instanceof ElectionMessageEvent) {
//                        ElectionMessageEvent event1 = (ElectionMessageEvent) event;
//                        final int sendingSubnodeId = event1.getSendingSubnodeId();
//                        final int receivingNodeId = event1.getReceivingNodeId();
//                        final Subnode sendingSubnode = subnodes.get(sendingSubnodeId);
//                        final int sendingNodeId = sendingSubnode.getNodeId();
//                        if (sendingNodeId != leader) {
//                            LOG.debug("need leader message event: {}", event.toString());
//                            addEvent(event);
//                            continue;
//                        }
//                    }
                    if (event.execute()) {
                        ++totalExecuted;
                        recordProperties(totalExecuted, startTime, event);
                    }
                }
                // wait whenever an election ends
                waitAllNodesVoted();
//                leaderElectionVerifier.verify();
                waitAllNodesSteadyBeforeRequest();

            }
            statistics.endTimer();
            // check election results
            leaderElectionVerifier.verify();
            statistics.reportTotalExecutedEvents(totalExecuted);
            statisticsWriter.write(statistics.toString() + "\n\n");
            LOG.info(statistics.toString() + "\n\n\n\n\n");

        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }


    /***
     * pre-steps to trigger phantom read
     * @param totalExecuted the first step sequence number
     * @return the last step sequence number
     */
    private int followersRestartAndLeaderCrash(int totalExecuted) {
        try {
            Event event;
            long startTime;
            synchronized (controlMonitor){
                // follower crash and restart first, then leader crash
                // distinguish leader and followers
                int leader = -1;
                List<Integer> followersId = new ArrayList<>();
                for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); ++nodeId) {
                    switch (leaderElectionStates.get(nodeId)) {
                        case LEADING:
                            leader = nodeId;
                            LOG.debug("-----current leader: {}", nodeId);
                            break;
                        case FOLLOWING:
                            LOG.debug("-----find a follower: {}", nodeId);
                            followersId.add(nodeId);
                    }
                }

                // each follower crash and restart
                for (int followerId: followersId) {
                    startTime = System.currentTimeMillis();
                    event = new NodeCrashEvent(generateEventId(), followerId, nodeCrashExecutor);
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                    LOG.debug("prepare to execute event: {}", event);
                    if (event.execute()) {
                        ++totalExecuted;
                        recordProperties(totalExecuted, startTime, event);
                    }

                    startTime = System.currentTimeMillis();
                    event = schedulingStrategy.nextEvent();
                    while (true) {
                        if (event instanceof NodeStartEvent) {
                            final int nodeId = ((NodeStartEvent) event).getNodeId();
                            LeaderElectionState RoleOfCrashNode = leaderElectionStates.get(nodeId);
                            LOG.debug("----previous role of this crashed node {}: {}---------", nodeId, RoleOfCrashNode);
                            if (nodeId == followerId){
                                break;
                            }
                        }
                        LOG.debug("-------need NodeStartEvent of the previous crashed follower! add this event back: {}", event);
                        addEvent(event);
                        event = schedulingStrategy.nextEvent();
                    }
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                    LOG.debug("prepare to execute event: {}", event);
                    if (event.execute()) {
                        ++totalExecuted;
                        recordProperties(totalExecuted, startTime, event);
                    }
                }

                // leader crash
                startTime = System.currentTimeMillis();
                event = new NodeCrashEvent(generateEventId(), leader, nodeCrashExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }

                // Do not let the crashed leader restart
                while (schedulingStrategy.hasNextEvent() && totalExecuted < 100) {
                    startTime = System.currentTimeMillis();
                    event = schedulingStrategy.nextEvent();
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                    LOG.debug("prepare to execute event: {}", event.toString());
                    if (event instanceof NodeStartEvent) {
                        final int nodeId = ((NodeStartEvent) event).getNodeId();
                        LeaderElectionState RoleOfCrashNode = leaderElectionStates.get(nodeId);
                        LOG.debug("----previous role of this crashed node {}: {}---------", nodeId, RoleOfCrashNode);
                        if ( LeaderElectionState.LEADING.equals(RoleOfCrashNode)){
                            ((NodeStartEvent) event).setExecuted();
                            LOG.debug("----Do not let the previous leader restart here! So pass this event---------\n\n\n");
                            continue;
                        }
                    }
                    if (event.execute()) {
                        ++totalExecuted;
                        recordProperties(totalExecuted, startTime, event);
                    }
                }
                // wait whenever an election ends
                waitAllNodesVoted();
                waitAllNodesSteadyBeforeRequest();
            }
            statistics.endTimer();
            // check election results
            leaderElectionVerifier.verify();
            statistics.reportTotalExecutedEvents(totalExecuted);
            statisticsWriter.write(statistics.toString() + "\n\n");
            LOG.info(statistics.toString() + "\n\n\n\n\n");

        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    /***
     * only issue GET_DATA without waiting for response
     */
    private int getDataTestNoWait(int totalExecuted, final int clientId) {
        try {
            long startTime;
            Event event;
            synchronized (controlMonitor) {
                // PRE
                // >> client get request
                startTime = System.currentTimeMillis();
                event = new ClientRequestEvent(generateEventId(), clientId,
                        ClientRequestType.GET_DATA, clientRequestExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    private int getDataTest(int totalExecuted, final int clientId) {
        try {
            long startTime;
            Event event;
            synchronized (controlMonitor) {
                // PRE
                // >> client get request
                startTime = System.currentTimeMillis();
                event = new ClientRequestEvent(generateEventId(), clientId,
                        ClientRequestType.GET_DATA, clientRequestWaitingResponseExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    public void addEvent(final Event event) {
        schedulingStrategy.add(event);
    }

    @Override
    public int offerElectionMessage(final int sendingSubnodeId, final int receivingNodeId, final Set<Integer> predecessorMessageIds, final String payload) {
        final List<Event> predecessorEvents = new ArrayList<>();
//        for (final int messageId : predecessorMessageIds) {
//            predecessorEvents.add(messageEventMap.get(messageId));
//        }
        final Subnode sendingSubnode = subnodes.get(sendingSubnodeId);
        final int sendingNodeId = sendingSubnode.getNodeId();
//        getSubNodeByID.put(sendingSubnodeId, sendingNodeId);

//        // TODO: mute the effect of events before adding it to the scheduling list. Why this makes election cost more time?
//        // Problem: It will make the message that come immediately after the node restarts to be missed
        if (partitionMap.get(sendingNodeId).get(receivingNodeId)){
            return TestingDef.RetCode.NODE_PAIR_IN_PARTITION;
        }

        // We want to determinize the order in which the first messages are added, so we wait until
        // all nodes with smaller ids have offered their first message.
        synchronized (controlMonitor) {
            if (sendingNodeId > 0 && firstMessage.get(sendingNodeId - 1) == null) {
                waitFirstMessageOffered(sendingNodeId - 1);
            }
        }

//        final NodeStartEvent lastNodeStartEvent = lastNodeStartEvents.get(sendingNodeId);
//        if (null != lastNodeStartEvent) {
//            predecessorEvents.add(lastNodeStartEvent);
//        }

        int id = generateEventId();
        final ElectionMessageEvent electionMessageEvent = new ElectionMessageEvent(id, sendingSubnodeId, receivingNodeId, payload, electionMessageExecutor);
        electionMessageEvent.addAllDirectPredecessors(predecessorEvents);

        synchronized (controlMonitor) {
            LOG.debug("Node {} is offering a message: msgId = {}, predecessors = {}, " +
                            "set subnode {} to SENDING state", sendingNodeId,
                    id, predecessorMessageIds.toString(), sendingSubnodeId);
            messageEventMap.put(id, electionMessageEvent);
            addEvent(electionMessageEvent);
            if (firstMessage.get(sendingNodeId) == null) {
                firstMessage.set(sendingNodeId, true);
            }
            sendingSubnode.setState(SubnodeState.SENDING);
            controlMonitor.notifyAll();
//            waitMessageReleased(id, sendingNodeId, receivingNodeId);
            waitMessageReleased(id, sendingNodeId);

            // normally, this event is released when scheduled except:
            // case 1: this event is released since the sending node is stopping
            if (NodeState.STOPPING.equals(nodeStates.get(sendingNodeId)) ) {
                id = TestingDef.RetCode.NODE_CRASH;
                electionMessageEvent.setExecuted();
                schedulingStrategy.remove(electionMessageEvent);
            }
            // case 2: this event is released when the network partition occurs
            // TODO: move partition effects to the beginning of this method. Use around() to mute the effect
            else if (partitionMap.get(sendingNodeId).get(receivingNodeId)) {
                id = TestingDef.RetCode.NODE_PAIR_IN_PARTITION;
                electionMessageEvent.setExecuted();
                schedulingStrategy.remove(electionMessageEvent);
            }

        }

        return id;
    }

    @Override
    public int offerFollowerToLeaderMessage(int sendingSubnodeId, long zxid, String payload, int type) throws RemoteException {
        LOG.debug("type: {}", type);

        final Subnode sendingSubnode = subnodes.get(sendingSubnodeId);
        final int sendingNodeId = sendingSubnode.getNodeId();

        Phase phase = nodePhases.get(sendingNodeId);
        if (phase.equals(Phase.BROADCAST)) {
            if (!clientInitializationDone) {
                LOG.debug("----client initialization is not done!---");
                return TestingDef.RetCode.CLIENT_INITIALIZATION_NOT_DONE;
            }
        }
        final List<Event> predecessorEvents = new ArrayList<>();

        // check who is the leader
        assert leaderElectionStates.contains(LeaderElectionState.LEADING);
        final int receivingNodeId = leaderElectionStates.indexOf(LeaderElectionState.LEADING);

        // mute the effect of events before adding it to the scheduling list
        if (partitionMap.get(sendingNodeId).get(receivingNodeId)){
            return TestingDef.RetCode.NODE_PAIR_IN_PARTITION;
        }

        int id = generateEventId();
        final FollowerToLeaderMessageEvent messageEvent = new FollowerToLeaderMessageEvent(
                id, sendingSubnodeId, receivingNodeId, type, zxid, payload, followerToLeaderMessageExecutor);
        messageEvent.addAllDirectPredecessors(predecessorEvents);

        synchronized (controlMonitor) {
            LOG.debug("Follower {} is offering a a type={} message to leader {}: msgId = {}, " +
                    "set subnode {} to SENDING state", sendingNodeId, type, receivingNodeId, id, sendingSubnodeId);

            addEvent(messageEvent);
            sendingSubnode.setState(SubnodeState.SENDING);
            controlMonitor.notifyAll();

//            waitMessageReleased(id, sendingNodeId, receivingNodeId);
            waitMessageReleased(id, sendingNodeId);

            // normally, this event is released when scheduled except:
            // case 1: this event is released since the sending node is stopping
            if (NodeState.STOPPING.equals(nodeStates.get(sendingNodeId)) ) {
                id = TestingDef.RetCode.NODE_CRASH;
                messageEvent.setExecuted();
                schedulingStrategy.remove(messageEvent);
            }
            // case 2: this event is released when the network partition occurs
            else if (partitionMap.get(sendingNodeId).get(receivingNodeId)) {
                sendingSubnode.setState(SubnodeState.RECEIVING);
                id = TestingDef.RetCode.NODE_PAIR_IN_PARTITION;
                messageEvent.setExecuted();
                schedulingStrategy.remove(messageEvent);
            }
        }
        return id;
    }

    @Override
    public int offerLeaderToFollowerMessage(int sendingSubnodeId, String receivingAddr, long zxid, String payload, int type) throws RemoteException {
//        if (!clientInitializationDone) {
//            LOG.debug("----client initialization is not done!---");
//            return TestingDef.RetCode.CLIENT_INITIALIZATION_NOT_DONE;
//        }
        LOG.debug("type: {}", type);

        final Subnode sendingSubnode = subnodes.get(sendingSubnodeId);
        final int sendingNodeId = sendingSubnode.getNodeId();

        Phase phase = nodePhases.get(sendingNodeId);

        if (phase.equals(Phase.BROADCAST)) {
            if (!clientInitializationDone) {
                LOG.debug("----client initialization is not done!---");
                return TestingDef.RetCode.CLIENT_INITIALIZATION_NOT_DONE;
            }
        }
        final List<Event> predecessorEvents = new ArrayList<>();

        int receivingNodeId;
        synchronized (controlMonitor) {
            waitFollowerSocketAddrRegistered(receivingAddr);
            receivingNodeId = followerSocketAddressBook.indexOf(receivingAddr);
            LOG.debug("receivingNodeId: {}, {}", receivingNodeId, receivingAddr);
            switch (type) {
//                case MessageType.DIFF:
//                case MessageType.TRUNC:
//                case MessageType.SNAP:
//                    // LearnerHandler
//                    followerLearnerHandlerMap.set(receivingNodeId, sendingSubnodeId);
//                    break;
                case MessageType.NEWLEADER:
                    assert leaderSyncFollowerCountMap.containsKey(sendingNodeId);
                    leaderSyncFollowerCountMap.put(sendingNodeId, leaderSyncFollowerCountMap.get(sendingNodeId) - 1);
                    // LearnerHandlerSender
                    followerLearnerHandlerSenderMap.set(receivingNodeId, sendingSubnodeId);
                    break;
                case MessageType.UPTODATE:
                case MessageType.PROPOSAL:
                case MessageType.COMMIT:
                    // LearnerHandlerSender
                    followerLearnerHandlerSenderMap.set(receivingNodeId, sendingSubnodeId);
                    break;
            }
        }

        // mute the effect of events before adding it to the scheduling list
        if (partitionMap.get(sendingNodeId).get(receivingNodeId)){
            return TestingDef.RetCode.NODE_PAIR_IN_PARTITION;
        }

        int id = generateEventId();
        final LeaderToFollowerMessageEvent messageEvent = new LeaderToFollowerMessageEvent(
                id, sendingSubnodeId, receivingNodeId, type, zxid, payload, leaderToFollowerMessageExecutor);
        messageEvent.addAllDirectPredecessors(predecessorEvents);

        synchronized (controlMonitor) {
            LOG.debug("Leader {} is offering a a type={} message to follower {}: msgId = {}, " +
                    "set subnode {} to SENDING state", sendingNodeId, type, receivingNodeId, id, sendingSubnodeId);

            addEvent(messageEvent);
            sendingSubnode.setState(SubnodeState.SENDING);
            controlMonitor.notifyAll();

//            waitMessageReleased(id, sendingNodeId, receivingNodeId);
            waitMessageReleased(id, sendingNodeId);

            // normally, this event is released when scheduled except:
            // case 1: this event is released since the sending node is stopping
            if (NodeState.STOPPING.equals(nodeStates.get(sendingNodeId)) ) {
                id = TestingDef.RetCode.NODE_CRASH;
                messageEvent.setExecuted();
                schedulingStrategy.remove(messageEvent);
            }
            // case 2: this event is released when the network partition occurs
            else if (partitionMap.get(sendingNodeId).get(receivingNodeId)) {
                sendingSubnode.setState(SubnodeState.RECEIVING);
                id = TestingDef.RetCode.NODE_PAIR_IN_PARTITION;
                messageEvent.setExecuted();
                schedulingStrategy.remove(messageEvent);
            }
        }
        return id;
    }

    @Override
    public int offerLocalEvent(int subnodeId, SubnodeType subnodeType, long zxid, String payload, int type) throws RemoteException {
        if (!subnodeType.equals(SubnodeType.QUORUM_PEER)) {
            if (!clientInitializationDone) {
                LOG.debug("----client initialization is not done!---");
                return TestingDef.RetCode.CLIENT_INITIALIZATION_NOT_DONE;
            }
        }

        final Subnode subnode = subnodes.get(subnodeId);
        final int nodeId = subnode.getNodeId();
//        getSubNodeByID.put(subnodeId, nodeId);

        int id = generateEventId();
        final LocalEvent localEvent =
                new LocalEvent(id, nodeId, subnodeId, subnodeType, payload, zxid, type, localEventExecutor);
        synchronized (controlMonitor) {
            LOG.debug("{} {} of Node {} is about to process the request ({}): msgId = {}, " +
                    "set subnode {} to SENDING state", subnodeType, subnodeId, nodeId, payload, id, subnodeId);
            addEvent(localEvent);
            subnode.setState(SubnodeState.SENDING);
            if (SubnodeType.COMMIT_PROCESSOR.equals(subnodeType)) {
                zxidToCommitMap.put(zxid, zxidToCommitMap.getOrDefault(zxid, 0) + 1);
            }
            controlMonitor.notifyAll();
//            waitLogRequestReleased(id, nodeId);
            waitMessageReleased(id, nodeId);

            // If this message is released due to the STOPPING node state, then set the id = -1
            if (NodeState.STOPPING.equals(nodeStates.get(nodeId))) {
                LOG.debug("----------setting localEvent executed. {}", localEvent);
                id = TestingDef.RetCode.NODE_CRASH;
                localEvent.setExecuted();
                schedulingStrategy.remove(localEvent);
            }
        }
        return id;
    }

    // Should be called while holding a lock on controlMonitor
    /***
     * For leader election
     * set sendingSubnode and receivingSubnode WORKER_RECEIVER to PROCESSING
     * @param event
     */
    public void releaseMessage(final ElectionMessageEvent event) {
        messageInFlight = event.getId();
        final Subnode sendingSubnode = subnodes.get(event.getSendingSubnodeId());
        // set the sending subnode to be PROCESSING
        sendingSubnode.setState(SubnodeState.PROCESSING);
        for (final Subnode subnode : subnodeSets.get(event.getReceivingNodeId())) {
            // ATTENTION: this is only for election
            if (subnode.getSubnodeType() == SubnodeType.WORKER_RECEIVER
                    && SubnodeState.RECEIVING.equals(subnode.getState())) {
                // set the receiving subnode to be PROCESSING
                subnode.setState(SubnodeState.PROCESSING);
                break;
            }
        }
        controlMonitor.notifyAll();
    }


    @Override
    public int registerSubnode(final int nodeId, final SubnodeType subnodeType) throws RemoteException {
        final int subnodeId;
        synchronized (controlMonitor) {
            subnodeId = subnodes.size();
            LOG.debug("-----------register subnode {} of node {}, type: {}--------", subnodeId, nodeId, subnodeType);
            final Subnode subnode = new Subnode(subnodeId, nodeId, subnodeType);
            subnodes.add(subnode);
            subnodeSets.get(nodeId).add(subnode);
        }
        return subnodeId;
    }

    @Override
    public void deregisterSubnode(final int subnodeId) throws RemoteException {
        synchronized (controlMonitor) {
            final Subnode subnode = subnodes.get(subnodeId);
            subnodeSets.get(subnode.getNodeId()).remove(subnode);
            if (!SubnodeState.UNREGISTERED.equals(subnode.getState())) {
                LOG.debug("---in deregisterSubnode, set {} {} of node {} UNREGISTERED",
                        subnode.getSubnodeType(), subnode.getId(), subnode.getNodeId());
                subnode.setState(SubnodeState.UNREGISTERED);
                // All subnodes may have become steady; give the scheduler a chance to make progress
                controlMonitor.notifyAll();
            }
        }
    }

    @Override
    public void registerFollowerSocketInfo(final int node, final String socketAddress) throws RemoteException {
        synchronized (controlMonitor) {
            followerSocketAddressBook.set(node, socketAddress);
            controlMonitor.notifyAll();
        }
    }

    @Override
    public void deregisterFollowerSocketInfo(int node) throws RemoteException {
        synchronized (controlMonitor) {
            followerSocketAddressBook.set(node, null);
            controlMonitor.notifyAll();
        }
    }

    @Override
    public void setProcessingState(final int subnodeId) throws RemoteException {
        synchronized (controlMonitor) {
            final Subnode subnode = subnodes.get(subnodeId);
            if (SubnodeState.RECEIVING.equals(subnode.getState())) {
                subnode.setState(SubnodeState.PROCESSING);
            }
        }
    }

    @Override
    public void setReceivingState(final int subnodeId) throws RemoteException {
        synchronized (controlMonitor) {
            final Subnode subnode = subnodes.get(subnodeId);
            if (SubnodeState.PROCESSING.equals(subnode.getState())) {
                subnode.setState(SubnodeState.RECEIVING);
                controlMonitor.notifyAll();
            }
        }
    }

    @Override
    public void nodeOnline(final int nodeId) throws RemoteException {
        synchronized (controlMonitor) {
            LOG.debug("--------- set online {}", nodeId);
            nodeStates.set(nodeId, NodeState.ONLINE);
            nodeStateForClientRequests.set(nodeId, NodeStateForClientRequest.SET_DONE);
            controlMonitor.notifyAll();
        }
    }

    @Override
    public void nodeOffline(final int nodeId) throws RemoteException {
        synchronized (controlMonitor) {
            LOG.debug("--------- set offline {}", nodeId);
            nodeStates.set(nodeId, NodeState.OFFLINE);
            nodeStateForClientRequests.set(nodeId, NodeStateForClientRequest.SET_DONE);
            controlMonitor.notifyAll();
        }
    }

    /***
     * Called by the executor of node start event
     * @param nodeId
     * @throws RemoteException
     */
    public void startNode(final int nodeId) throws RemoteException {
        // 1. PRE_EXECUTION: set unstable state (set STARTING)
        nodeStates.set(nodeId, NodeState.STARTING);
        nodePhases.set(nodeId, Phase.DISCOVERY);
        nodeStateForClientRequests.set(nodeId, NodeStateForClientRequest.SET_DONE);
        votes.set(nodeId, null);
        leaderElectionStates.set(nodeId, LeaderElectionState.LOOKING);
        followerSocketAddressBook.set(nodeId, null);
        followerLearnerHandlerMap.set(nodeId, null);
        followerLearnerHandlerSenderMap.set(nodeId, null);
        syncTypeList.set(nodeId, -1);


        // 2. EXECUTION
        ensemble.startNode(nodeId);
        // 3. POST_EXECUTION: wait for the state to be stable
        // the started node will call for remote service of nodeOnline(..)
        for (int id = 0; id < schedulerConfiguration.getNumNodes(); id++) {
            LOG.debug("--------nodeid: {}: phase: {}, leaderElectionState: {}",
                    id, nodePhases.get(id), leaderElectionStates.get(id));
        }
    }

    /***
     * Called by the executor of node crash event
     * @param nodeId
     */
    public void stopNode(final int nodeId) {
        // 1. PRE_EXECUTION: set unstable state (set STOPPING)
        boolean hasSending = false;
        for (final Subnode subnode : subnodeSets.get(nodeId)) {
            if (SubnodeState.SENDING.equals(subnode.getState())) {
                LOG.debug("----Node {} still has SENDING subnode {} {}: {}",
                        nodeId, subnode.getSubnodeType(), subnode.getId(), subnode.getState());
                hasSending = true;
                break;
            }
        }

//        // This will make the client session socket close immediately
//        if (leaderElectionStates.get(nodeId).equals(LeaderElectionState.FOLLOWING)) {
//            Integer learnerHandlerId = followerLearnerHandlerMap.get(nodeId);
//            Subnode learnerHandler = subnodes.get(learnerHandlerId);
//            if (SubnodeState.SENDING.equals(learnerHandler.getState())) {
//                LOG.debug("----Node {} still has SENDING subnode {} {}: {}",
//                        nodeId, learnerHandler.getSubnodeType(), learnerHandler.getId(), learnerHandler.getState());
//                hasSending = true;
//            }
//
//            Integer learnerHandlerSenderId = followerLearnerHandlerSenderMap.get(nodeId);
//            Subnode learnerHandlerSender = subnodes.get(learnerHandlerSenderId);
//            if (SubnodeState.SENDING.equals(learnerHandlerSender.getState())) {
//                LOG.debug("----Node {} still has SENDING subnode {} {}: {}",
//                        nodeId, learnerHandlerSender.getSubnodeType(), learnerHandlerSender.getId(), learnerHandlerSender.getState());
//                hasSending = true;
//            }
//        }

        // IF there exists any threads about to send a message, then set the corresponding event executed
        if (hasSending) {
            // STOPPING state will make the pending message to be released immediately
            nodeStates.set(nodeId, NodeState.STOPPING);
            controlMonitor.notifyAll();
            // Wait util no node is STARTING or STOPPING.
            // This node will be set to OFFLINE by the last existing thread that release a sending message
            waitAllNodesSteady();
        }

        // Subnode management
        for (final Subnode subnode : subnodeSets.get(nodeId)) {
            subnode.setState(SubnodeState.UNREGISTERED);
        }
        subnodeSets.get(nodeId).clear();
        LOG.debug("-------Node {} 's subnodes has been cleared.", nodeId);

        // If hasSending == true, the node has been set OFFLINE when the last intercepted subnode is shutdown
        // o.w. set OFFLINE here anyway.
        nodeStates.set(nodeId, NodeState.OFFLINE);
        nodePhases.set(nodeId, Phase.NULL);
        nodeStateForClientRequests.set(nodeId, NodeStateForClientRequest.SET_DONE);
        for (int id = 0; id < schedulerConfiguration.getNumNodes(); id++) {
            LOG.debug("--------nodeid: {}: phase: {}", id, nodePhases.get(id));

            // node crash will not reset the partition
//            // recover the partition with this node
//            partitionMap.get(nodeId).set(id, false);
//            partitionMap.get(id).set(nodeId, false);
        }

        votes.set(nodeId, null);
        // is it needed to still keep the role info before crash for further property check？
        leaderElectionStates.set(nodeId, LeaderElectionState.NULL);
        followerSocketAddressBook.set(nodeId, null);
        followerLearnerHandlerMap.set(nodeId, null);
        followerLearnerHandlerSenderMap.set(nodeId, null);
        syncTypeList.set(nodeId, -1);
        participants.remove(nodeId);

        // 2. EXECUTION
        ensemble.stopNode(nodeId);

        // 3. POST_EXECUTION: wait for the state to be stable
        // This has been done in step 1 (set OFFLINE)

    }

    public int generateEventId() {
        return eventIdGenerator.incrementAndGet();
    }

    public int generateClientId() {
        return clientIdGenerator.incrementAndGet();
    }

    @Override
    public void updateVote(final int nodeId, final Vote vote) throws RemoteException {
        synchronized (controlMonitor) {
            votes.set(nodeId, vote);
            controlMonitor.notifyAll();
            if(vote == null){
                return;
            }
            try {
                executionWriter.write("Node " + nodeId + " final vote: " + vote.toString() + '\n');
            } catch (final IOException e) {
                LOG.debug("IO exception", e);
            }
        }
    }

    @Override
    public void initializeVote(int nodeId, Vote vote) throws RemoteException {
        synchronized (controlMonitor) {
            votes.set(nodeId, vote);
            controlMonitor.notifyAll();
        }
    }

    @Override
    public void updateLeaderElectionState(final int nodeId, final LeaderElectionState state) throws RemoteException {
        LOG.debug("before setting Node {} state: {}", nodeId, state);
        synchronized (controlMonitor) {
            leaderElectionStates.set(nodeId, state);
            if (LeaderElectionState.LOOKING.equals(state)) {
                nodePhases.set(nodeId, Phase.DISCOVERY);
                votes.set(nodeId, null);
                followerSocketAddressBook.set(nodeId, null);
                followerLearnerHandlerMap.set(nodeId, null);
                followerLearnerHandlerSenderMap.set(nodeId, null);
                syncTypeList.set(nodeId, -1);
                participants.remove(nodeId);
            } else {
                nodePhases.set(nodeId, Phase.SYNC);
            }
            try {
                LOG.debug("Writing execution file------Node {} state: {}", nodeId, state);
                executionWriter.write("\nNode " + nodeId + " state: " + state + '\n');
            } catch (final IOException e) {
                LOG.debug("IO exception", e);
            }
            for (int id = 0; id < schedulerConfiguration.getNumNodes(); id++) {
                LOG.debug("--------nodeid: {}: phase: {}", id, nodePhases.get(id));
            }
            controlMonitor.notifyAll();
        }
        LOG.debug("after setting Node {} state: {}", nodeId, state);
    }

    @Override
    public void initializeLeaderElectionState(int nodeId, LeaderElectionState state) throws RemoteException {
        synchronized (controlMonitor) {
            leaderElectionStates.set(nodeId, state);
            try {
                LOG.debug("Node {} initialized state: {}", nodeId, state);
                executionWriter.write("Node " + nodeId + " initialized state: " + state + '\n');
            } catch (final IOException e) {
                LOG.debug("IO exception", e);
            }
        }
    }

    @Override
    public void updateLastProcessedZxid(int nodeId, long lastProcessedZxid) throws RemoteException{
        synchronized (controlMonitor) {
            lastProcessedZxids.set(nodeId, lastProcessedZxid);
            nodeStateForClientRequests.set(nodeId, NodeStateForClientRequest.SET_DONE);

            try {
                // Update allZxidRecords
                List<Long> zxidRecord = allZxidRecords.get(nodeId);
                int len = zxidRecord.size();
                LOG.debug("Node " + nodeId + " record: {}", zxidRecord);
                // TODO: buggy
//                if (syncTypeList.get(nodeId).equals(MessageType.SNAP) || syncTypeList.get(nodeId).equals(MessageType.TRUNC)) {
//                    LOG.debug("{}, {}", lastProcessedZxid, lastCommittedZxid);
////                    allZxidRecords.clear();
////                    for (Long zxid: lastCommittedZxid) {
////                        zxidRecord.add(zxid);
////                    }
//                    int idx = lastCommittedZxid.indexOf(lastProcessedZxid);
//                    allZxidRecords.set(nodeId, new ArrayList<>(lastCommittedZxid.subList(0, idx+1)));
//                    LOG.debug("syncTypeList({}): SNAP / TRUNC, {}, {}", nodeId, zxidRecord, allZxidRecords.get(nodeId));
//                    syncTypeList.set(nodeId, -1);
//                    executionWriter.write(
//                            "\n---just update Node " + nodeId + "'s last record: " + allZxidRecords.get(nodeId));
//                } else
                if (zxidRecord.get(len - 1) < lastProcessedZxid
                        && (lastProcessedZxid & 0xffffffffL) != 0L  ){
                    zxidRecord.add(lastProcessedZxid);
//                allZxidRecords.get(nodeId).add(lastProcessedZxid);
                    executionWriter.write(
                            "\n---just update Node " + nodeId + "'s last record: " + allZxidRecords.get(nodeId));
                }

                // Update lastCommittedZxid by leader
                if (NodeState.ONLINE.equals(nodeStates.get(nodeId))
                        && LeaderElectionState.LEADING.equals(leaderElectionStates.get(nodeId))
                        && Phase.BROADCAST.equals(nodePhases.get(nodeId))) {

                    lastCommittedZxid.add(lastProcessedZxid);
                    executionWriter.write(
                            "\n---Update lastCommittedZxid " + lastCommittedZxid);
                }

                executionWriter.write(
                        "\n---Update Node " + nodeId + "'s lastProcessedZxid: 0x" + Long.toHexString(lastProcessedZxid));
//                for (int i = 0; i < schedulerConfiguration.getNumNodes(); i++) {
//                    executionWriter.write(" # " + Long.toHexString(lastProcessedZxids.get(i)));
//                }
                executionWriter.write(
                        "\n---Node " + nodeId + "'s last record: " + allZxidRecords);
                executionWriter.write("\n");
                controlMonitor.notifyAll();
            } catch (final IOException e) {
                LOG.debug("IO exception", e);
            }
        }
    }

    public void recordProperties(final int step, final long startTime, final Event event) throws IOException {
        executionWriter.write("\n---Step: " + step + "--->");
        executionWriter.write(event.toString());
        executionWriter.write("\nlastProcessedZxid: 0x");
        // Property verification
        for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); nodeId++) {
            executionWriter.write(Long.toHexString(lastProcessedZxids.get(nodeId)) + " # ");
        }
        executionWriter.write("\ntime/ms: " + (System.currentTimeMillis() - startTime) + "\n");
        executionWriter.flush();
    }

    public void updateResponseForClientRequest(ClientRequestEvent event) throws IOException {
        executionWriter.write("\n---Get response of " + event.getType() + ": ");
        executionWriter.write(event.toString() + "\n");
        executionWriter.flush();
    }

    @Override
    public void readyForBroadcast(int subnodeId) throws RemoteException {
        // TODO: Leader needs to collect quorum. Here we suppose 1 learnerHanlder is enough for 3-node ensemble
        final Subnode subnode = subnodes.get(subnodeId);
        final int nodeId = subnode.getNodeId();
        LOG.debug("Node {} is ready to broadcast", nodeId);
        synchronized (controlMonitor) {
            nodePhases.set(nodeId, Phase.BROADCAST);
            controlMonitor.notifyAll();

            for (int id = 0; id < schedulerConfiguration.getNumNodes(); id++) {
                LOG.debug("--------nodeid: {}: phase: {}", id, nodePhases.get(id));
            }
        }
    }


    /***
     * The following predicates are general to some type of events.
     * Should be called while holding a lock on controlMonitor
     */

    /***
     * wait for client session initialization finished
     */
    public void waitClientSessionReady(final int clientId) {
        final WaitPredicate clientSessionReady = new ClientSessionReady(this, clientId);
        wait(clientSessionReady, 0L);
    }

    /***
     * General pre-/post-condition
     */
    private final WaitPredicate allNodesSteady = new AllNodesSteady(this);
    public void waitAllNodesSteady() {
        wait(allNodesSteady, 0L);
    }

    /***
     * Pre-condition for election vote property check
     */
    private final WaitPredicate allNodesVoted = new AllNodesVoted(this);
    // Should be called while holding a lock on controlMonitor
    private boolean waitAllNodesVoted() {
        wait(allNodesVoted, 100L);
        for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); ++nodeId) {
            if (LeaderElectionState.LEADING.equals(leaderElectionStates.get(nodeId))) {
                return true;
            }
        }
        LOG.debug("Leader does not exist after voting!");
        return false;
    }

    /***
     * Pre-condition for election vote property check for particular peers
     */
    // Should be called while holding a lock on controlMonitor
    private boolean waitAllParticipantsVoted(final Set<Integer> participants) {
        final WaitPredicate allParticipantsVoted = new AllNodesVoted(this, participants);
        wait(allParticipantsVoted, 100L);
        for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); ++nodeId) {
            if (LeaderElectionState.LEADING.equals(leaderElectionStates.get(nodeId))) {
                return true;
            }
        }
        LOG.debug("Leader does not exist after voting!");
        return false;
    }

    /***
     * Pre-condition for scheduling an internal event
     */
    private Event waitTargetInternalEventReady(ExternalModelStrategy strategy,
                                                 ModelAction action,
                                                 Integer nodeId,
                                                 Integer peerId) {
        final TargetInternalEventReady targetInternalEventReady = new TargetInternalEventReady(this, strategy, action, nodeId, peerId);
        wait(targetInternalEventReady, 100L);
        Event e = targetInternalEventReady.getEvent();
        return e;
    }


    private void waitAllNodesVoted(final long timeout) {
        wait(allNodesVoted, timeout);
    }

    /***
     * Post-condition for election & Pre-condition for LeaderSyncFollowers
     */
    private void waitLeaderSyncReady(final int leaderId, List<Integer> peers) {
        WaitPredicate leaderSyncReady = new LeaderSyncReady(this, leaderId, peers);
        wait(leaderSyncReady, 0L);
    }

    /***
     * Pre-condition for client requests.
     * Note: session creation is all a type of client request, so this is better placed before client session creation.
     */
    private final WaitPredicate allNodesSteadyBeforeRequest = new AllNodesSteadyBeforeRequest(this);
    // Should be called while holding a lock on controlMonitor
    public void waitAllNodesSteadyBeforeRequest() {
        wait(allNodesSteadyBeforeRequest, 0L);
    }

    /***
     * Post-condition for a follower process UPTODATE.
     * Note: session creation is all a type of client request, so this is better placed before client session creation.
     */
    // Should be called while holding a lock on controlMonitor
    public void waitFollowerSteadyAfterProcessingUPTODATE(final int followerId) {
        final WaitPredicate followerSteadyAfterProcessingUPTODATE =
                new FollowerSteadyAfterProcessingUPTODATE(this, followerId);
        wait(followerSteadyAfterProcessingUPTODATE, 0L);
    }

    /***
     * Pre-condition for client mutation
     */
    private final WaitPredicate allNodesSteadyBeforeMutation = new AllNodesSteadyBeforeMutation(this);
    @Deprecated
    public void waitAllNodesSteadyBeforeMutation() {
        wait(allNodesSteadyBeforeMutation, 0L);
    }

    /***
     * Post-condition for client mutation
     */
    private final WaitPredicate allNodesSteadyAfterMutation = new AllNodesSteadyAfterMutation(this);
    public void waitAllNodesSteadyAfterMutation() {wait(allNodesSteadyAfterMutation, 0L);}


    /***
     * Pre-condition for logging
     */
    private final WaitPredicate allNodesLogSyncSteady = new AllNodesLogSyncSteady(this);
    public void waitAllNodesLogSyncSteady() {
        wait(allNodesLogSyncSteady, 0L);
    }

    private final WaitPredicate allNodesSteadyAfterQuorumSynced = new AllNodesSteadyAfterQuorumSynced(this);
    public void waitAllNodesSteadyAfterQuorumSynced() {
        wait(allNodesSteadyAfterQuorumSynced, 0L);
    }


    /**
     * The following predicates are specific to an event.
     */


    public void waitResponseForClientRequest(ClientRequestEvent event) {
        final WaitPredicate responseForClientRequest = new ResponseForClientRequest(this, event);
        wait(responseForClientRequest, 0L);
    }

    /***
     * Pre-condition for scheduling the first event in the election
     * @param nodeId
     */
    private void waitFirstMessageOffered(final int nodeId) {
        final WaitPredicate firstMessageOffered = new FirstMessageOffered(this, nodeId);
        wait(firstMessageOffered, 0L);
    }

    private void waitNewMessageOffered() {
        final WaitPredicate newMessageOffered = new NewMessageOffered(this);
        wait(newMessageOffered, 0L);
    }

    /***
     * Pre-condition for scheduling the message event
     * Including: notification message in the election, leader-to-follower message, log message
     * @param msgId
     * @param sendingNodeId
     */
    private void waitMessageReleased(final int msgId, final int sendingNodeId) {
        final WaitPredicate messageReleased = new MessageReleased(this, msgId, sendingNodeId);
        wait(messageReleased, 0L);
    }

    private void waitMessageReleased(final int msgId, final int sendingNodeId, final int receivingNodeId) {
        final WaitPredicate messageReleased = new MessageReleased(this, msgId, sendingNodeId, receivingNodeId);
        wait(messageReleased, 0L);
    }

    /***
     * Pre-condition for scheduling the log message event
     * @param msgId
     * @param syncNodeId
     */
    private void waitLogRequestReleased(final int msgId, final int syncNodeId) {
        final WaitPredicate logRequestReleased = new LogRequestReleased(this, msgId, syncNodeId);
        wait(logRequestReleased, 0L);
    }

    /***
     * Post-condition for scheduling the commit message event
     * @param msgId
     * @param nodeId
     */
    public void waitCommitProcessorDone(final int msgId, final int nodeId) {
        final long zxid = lastProcessedZxids.get(nodeId);
        final WaitPredicate commitProcessorDone = new CommitProcessorDone(this, msgId, nodeId, zxid);
        wait(commitProcessorDone, 0L);
    }

    /***
     * Note: this is used when learnerHandler's COMMIT message is not targeted.
     * o.w. use global predicate AllNodesSteadyAfterQuorumSynced
     * Post-condition for scheduling quorum log message events
     */
    public void waitQuorumToCommit(final LocalEvent event) {
        final long zxid = event.getZxid();
        final int nodeNum = schedulerConfiguration.getNumNodes();
        final WaitPredicate quorumToCommit = new QuorumToCommit(this, zxid, nodeNum);
        wait(quorumToCommit, 0L);
    }

    /***
     * Pre-condition for scheduling the leader-to-follower message event
     * @param addr
     */
    private void waitFollowerSocketAddrRegistered(final String addr) {
        final WaitPredicate followerSocketAddrRegistered = new FollowerSocketAddrRegistered(this, addr);
        wait(followerSocketAddrRegistered, 0L);
    }

    private void waitAliveNodesInLookingState() {
        final WaitPredicate aliveNodesInLookingState = new AliveNodesInLookingState(this);
        wait(aliveNodesInLookingState, 0L);
    }

    private void waitAliveNodesInLookingState(final Set<Integer> peers) {
        final WaitPredicate aliveNodesInLookingState = new AliveNodesInLookingState(this, peers);
        wait(aliveNodesInLookingState, 0L);
    }


    public void waitFollowerMappingLearnerHandlerSender(final int subnodeId) {
        final WaitPredicate followerMappingLearnerHandlerSender = new FollowerMappingLearnerHandlerSender(this, subnodeId);
        wait(followerMappingLearnerHandlerSender, 0L);
    }

    /***
     * Post-condition for specific subnode in SENDING state
     */
    public void waitSubnodeInSendingState(final int subnodeId) {
        final WaitPredicate subnodeInSendingState = new SubnodeInSendingState(this, subnodeId);
        wait(subnodeInSendingState, 0L);
    }

    private void wait(final WaitPredicate predicate, final long timeout) {
        LOG.debug("Waiting for {}\n\n\n", predicate.describe());
        final long startTime = System.currentTimeMillis();
        long endTime = startTime;
        while (!predicate.isTrue() && (timeout == 0L || endTime - startTime < timeout)) {
            try {
                if (timeout == 0L) {
                    controlMonitor.wait();
                } else {
                    controlMonitor.wait(Math.max(1L, timeout - (endTime - startTime)));
                }
            } catch (final InterruptedException e) {
                LOG.debug("Interrupted from waiting on the control monitor");
            } finally {
                endTime = System.currentTimeMillis();
            }
        }
        LOG.debug("Done waiting for {}\n\n\n\n\n", predicate.describe());
    }

//    public int nodIdOfSubNode(int subNodeID){
//        return getSubNodeByID.get(subNodeID);
//    }
}