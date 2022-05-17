package org.mpisws.hitmc.server;

import org.mpisws.hitmc.api.*;
import org.mpisws.hitmc.api.configuration.SchedulerConfiguration;
import org.mpisws.hitmc.api.configuration.SchedulerConfigurationException;
import org.mpisws.hitmc.api.state.ClientRequestType;
import org.mpisws.hitmc.api.state.LeaderElectionState;
import org.mpisws.hitmc.api.state.Vote;
import org.mpisws.hitmc.server.checker.GetDataVerifier;
import org.mpisws.hitmc.server.checker.LeaderElectionVerifier;
import org.mpisws.hitmc.server.checker.TraceVerifier;
import org.mpisws.hitmc.server.event.*;
import org.mpisws.hitmc.server.executor.*;
import org.mpisws.hitmc.server.predicate.*;
import org.mpisws.hitmc.server.scheduler.*;
import org.mpisws.hitmc.server.state.Subnode;
import org.mpisws.hitmc.server.statistics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

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
    private MessageExecutor messageExecutor; // for election
    private RequestProcessorExecutor requestProcessorExecutor; // for logging
    private LearnerHandlerMessageExecutor learnerHandlerMessageExecutor; // for learnerHandler-follower

    // statistics
    private Statistics statistics;

    // checker
    private TraceVerifier traceVerifier;
    private LeaderElectionVerifier leaderElectionVerifier;
    private GetDataVerifier getDataVerifier;

    // writer
    private FileWriter statisticsWriter;
    private FileWriter executionWriter;
    //private FileWriter vectorClockWriter;

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

    private final List<NodeStateForClientRequest> nodeStateForClientRequests = new ArrayList<>();

    // properties
    private final List<Long> lastProcessedZxids = new ArrayList<>();

    private final List<Vote> votes = new ArrayList<>();
    private final List<LeaderElectionState> leaderElectionStates = new ArrayList<>();

    private final List<Integer> returnedDataList = new ArrayList<>();

    private final Map<Long, Integer> zxidSyncedMap = new HashMap<>();
    private final Map<Long, Integer> zxidToCommitMap = new HashMap<>();

    public boolean traceMatched;
    public boolean tracePassed;

    // event
    private final AtomicInteger eventIdGenerator = new AtomicInteger();

    private final AtomicInteger clientIdGenerator = new AtomicInteger();

    // for event dependency
    private final Map<Integer, MessageEvent> messageEventMap = new HashMap<>();
//    private final Map<Integer, LearnerHandlerMessageEvent> followerMessageEventMap = new HashMap<>();

    private final List<NodeStartEvent> lastNodeStartEvents = new ArrayList<>();
    // TODO: firstMessage should be renewed whenever election occurs.
    private final List<Boolean> firstMessage = new ArrayList<>();

    private int messageInFlight;

    private int logRequestInFlight;

    public List<Integer> getReturnedDataList() {
        return returnedDataList;
    }

    public Map<Long, Integer> getZxidSyncedMap() {
        return zxidSyncedMap;
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
            totalExecuted = scheduleElection(null, totalExecuted);

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

        long startTime = System.currentTimeMillis();
        int traceNum = externalModelStrategy.getTracesNum();
        LOG.debug("traceNum: {}", traceNum);

        for (int executionId = 1; executionId <= traceNum; ++executionId) {
            externalModelStrategy.clearEvents();
            schedulingStrategy = externalModelStrategy;
            statistics = externalModelStatistics;

            Trace trace = externalModelStrategy.getCurrentTrace(executionId - 1);
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
            try {
                for (; currentStep <= stepCount; ++currentStep) {
                    String line = trace.nextStep();
                    LOG.debug("nextStep: {}", line);

                    String[] lineArr = line.split(" ");
                    int len = lineArr.length;

                    String action = lineArr[0];
                    switch (action) {
                        case "ELECTION":
                            Integer leaderId = len > 1 ? Integer.parseInt(lineArr[1]) : null;
                            LOG.debug("election leader: {}", leaderId);
                            totalExecuted = scheduleElection(leaderId, totalExecuted);
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
                            totalExecuted = scheduleInternalEvent(externalModelStrategy, lineArr, totalExecuted);
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
                            totalExecuted = scheduleGetData(getDataClientId, sid, result, totalExecuted);
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
                LOG.info("SchedulerConfigurationException found when scheduling Trace {} in Step {} / {}. " +
                                "Complete trace: {}", traceName, currentStep, stepCount, trace);
                tracePassed = false;
            } finally {
                statistics.endTimer();

                // report statistics of total trace
                traceVerifier.setTraceLen(stepCount);
                traceVerifier.setExecutedStep(currentStep);
                traceVerifier.verify();
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

        // Configure external event executors
        nodeStartExecutor = new NodeStartExecutor(this, schedulerConfiguration.getNumReboots());
        nodeCrashExecutor = new NodeCrashExecutor(this, schedulerConfiguration.getNumCrashes());

        clientRequestWaitingResponseExecutor = new ClientRequestExecutor(this, true, 0);
        clientRequestExecutor = new ClientRequestExecutor(this, false, 0);

        partitionStartExecutor = new PartitionStartExecutor(this, 10);
        partitionStopExecutor = new PartitionStopExecutor(this, 10);

        // Configure internal event executors
        messageExecutor = new MessageExecutor(this);
        requestProcessorExecutor = new RequestProcessorExecutor(this);
        learnerHandlerMessageExecutor = new LearnerHandlerMessageExecutor(this);

        // Configure checkers
        traceVerifier = new TraceVerifier(this, statistics);
        leaderElectionVerifier = new LeaderElectionVerifier(this, statistics);
        getDataVerifier = new GetDataVerifier(this, statistics);

        // for property check
        votes.clear();
        votes.addAll(Collections.<Vote>nCopies(schedulerConfiguration.getNumNodes(), null));

        leaderElectionStates.clear();
        leaderElectionStates.addAll(Collections.nCopies(schedulerConfiguration.getNumNodes(), LeaderElectionState.LOOKING));

        returnedDataList.clear();
        returnedDataList.add(0);

        zxidSyncedMap.clear();
        zxidToCommitMap.clear();

        traceMatched = true;
        tracePassed = true;

        // Configure nodes and subnodes
        lastProcessedZxids.clear();
        nodeStates.clear();
        nodePhases.clear();

        subnodeSets.clear();
        subnodes.clear();
        nodeStateForClientRequests.clear();
        followerSocketAddressBook.clear();

        for (int i = 0 ; i < schedulerConfiguration.getNumNodes(); i++) {
            nodeStates.add(NodeState.STARTING);
            nodePhases.add(Phase.DISCOVERY);
            subnodeSets.add(new HashSet<Subnode>());
            lastProcessedZxids.add(0L);
            nodeStateForClientRequests.add(NodeStateForClientRequest.SET_DONE);
            followerSocketAddressBook.add(null);
        }

        // configure client map
        clientMap.clear();

        // configure network partion info
        partitionMap.clear();
        final int nodeNum = schedulerConfiguration.getNumNodes();
        for (int i = 0 ; i < nodeNum; i++) {
            partitionMap.add(new ArrayList<>(Collections.nCopies(schedulerConfiguration.getNumNodes(), false)));
        }
//        partitionMap.addAll(Collections.nCopies(schedulerConfiguration.getNumNodes(),
//                new ArrayList<>(Collections.nCopies(schedulerConfiguration.getNumNodes(), false))));

        eventIdGenerator.set(0);
        clientIdGenerator.set(0);

        messageEventMap.clear();
        messageInFlight = 0;
        logRequestInFlight = 0;

        firstMessage.clear();
        firstMessage.addAll(Collections.<Boolean>nCopies(schedulerConfiguration.getNumNodes(), null));

        // Configure lastNodeStartEvents
        lastNodeStartEvents.clear();
        lastNodeStartEvents.addAll(Collections.<NodeStartEvent>nCopies(schedulerConfiguration.getNumNodes(), null));

        // Generate node crash events
        if (schedulerConfiguration.getNumCrashes() > 0) {
            for (int i = 0; i < schedulerConfiguration.getNumNodes(); i++) {
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
     * The schedule process of leader election
     * Pre-condition: all nodes steady
     * Post-condition: all nodes voted & all nodes steady before request
     * Property check: one leader has been elected
     * @param leaderId the leader that model specifies
     * @param totalExecuted the number of previous executed events
     * @return the number of executed events
     */
    // TODO: add partition events during election
    public int scheduleElection(Integer leaderId, int totalExecuted) {
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
                    if (event instanceof LearnerHandlerMessageEvent) {
                        final int sendingSubnodeId = ((LearnerHandlerMessageEvent) event).getSendingSubnodeId();
                        // confirm this works / use partition / let
                        deregisterSubnode(sendingSubnodeId);
                        ((LearnerHandlerMessageEvent) event).setExecuted();
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
    private void establishSession(final int clientId, final boolean resetConnectionState, final String serverList) {
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
     * setData with specific data
     * if without specific data, will use eventId as its written string value
     */
    private int scheduleSetData(final int clientId, final int serverId, final String data, int totalExecuted) {
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
     * getData
     */
    private int scheduleGetData(final int clientId, final int serverId, final Integer modelResult, int totalExecuted) {
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
            statistics.reportTotalExecutedEvents(totalExecuted);
            statisticsWriter.write(statistics.toString() + "\n\n");
            LOG.info(statistics.toString() + "\n\n\n\n\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }


    public int scheduleInternalEvent(ExternalModelStrategy strategy, String[] lineArr, int totalExecuted) throws SchedulerConfigurationException {
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
                while (!(event instanceof RequestEvent) ||
                        !(SubnodeType.SYNC_PROCESSOR.equals(((RequestEvent) event).getSubnodeType())) ||
                        ((RequestEvent) event).getNodeId() != nodeId){
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
                while (!(event instanceof RequestEvent) ||
                        !(SubnodeType.COMMIT_PROCESSOR.equals(((RequestEvent) event).getSubnodeType())) ||
                        ((RequestEvent) event).getNodeId() != nodeId){
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
                while (!(event instanceof LearnerHandlerMessageEvent) ||
                        ((LearnerHandlerMessageEvent) event).getReceivingNodeId() != receivingNode){
                    LOG.debug("-------need node {} 's LearnerHandlerMessageEvent to node {}! get event: {}",
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
                while (!(event instanceof RequestEvent)){
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
                while (!(event instanceof RequestEvent)){
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
//                        if (event instanceof LearnerHandlerMessageEvent) {
//                            final int sendingSubnodeId = ((LearnerHandlerMessageEvent) event).getSendingSubnodeId();
//                            // confirm this works / use partition / let
//                            deregisterSubnode(sendingSubnodeId);
//                            ((LearnerHandlerMessageEvent) event).setExecuted();
//
////                        schedulingStrategy.remove(event);
//                            LOG.debug("----Do not let the previous learner handler message occur here! So pass this event---------\n\n\n");
//                            continue;
//                        }
//                        else if (!(event instanceof MessageEvent)) {
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
                    if (event instanceof LearnerHandlerMessageEvent) {
                        final int sendingSubnodeId = ((LearnerHandlerMessageEvent) event).getSendingSubnodeId();
                        // confirm this works / use partition / let
                        deregisterSubnode(sendingSubnodeId);
                        ((LearnerHandlerMessageEvent) event).setExecuted();
                        LOG.debug("----Do not let the previous learner handler message occur here! So pass this event---------\n\n\n");
                        continue;
                    }
//                    else if (event instanceof MessageEvent) {
//                        MessageEvent event1 = (MessageEvent) event;
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
    public int offerMessage(final int sendingSubnodeId, final int receivingNodeId, final Set<Integer> predecessorMessageIds, final String payload) {
        final List<Event> predecessorEvents = new ArrayList<>();
        for (final int messageId : predecessorMessageIds) {
            predecessorEvents.add(messageEventMap.get(messageId));
        }
        final Subnode sendingSubnode = subnodes.get(sendingSubnodeId);
        final int sendingNodeId = sendingSubnode.getNodeId();
//        getSubNodeByID.put(sendingSubnodeId, sendingNodeId);

        // We want to determinize the order in which the first messages are added, so we wait until
        // all nodes with smaller ids have offered their first message.
        synchronized (controlMonitor) {
            if (sendingNodeId > 0 && firstMessage.get(sendingNodeId - 1) == null) {
                waitFirstMessageOffered(sendingNodeId - 1);
            }
        }

        final NodeStartEvent lastNodeStartEvent = lastNodeStartEvents.get(sendingNodeId);
        if (null != lastNodeStartEvent) {
            predecessorEvents.add(lastNodeStartEvent);
        }

        int id = generateEventId();
        final MessageEvent messageEvent = new MessageEvent(id, sendingSubnodeId, receivingNodeId, payload, messageExecutor);
        messageEvent.addAllDirectPredecessors(predecessorEvents);

        synchronized (controlMonitor) {
            LOG.debug("Node {} is offering a message: msgId = {}, predecessors = {}, " +
                            "set subnode {} to SENDING state", sendingNodeId,
                    id, predecessorMessageIds.toString(), sendingSubnodeId);
            messageEventMap.put(id, messageEvent);
            addEvent(messageEvent);
            if (firstMessage.get(sendingNodeId) == null) {
                firstMessage.set(sendingNodeId, true);
            }
            sendingSubnode.setState(SubnodeState.SENDING);
            controlMonitor.notifyAll();
            waitMessageReleased(id, sendingNodeId);

            // normally, this event is released when scheduled except:
            // case 1: this event is released since the sending node is stopping
            if (NodeState.STOPPING.equals(nodeStates.get(sendingNodeId))) {
                id = TestingDef.RetCode.NODE_CRASH;
                messageEvent.setExecuted();
                schedulingStrategy.remove(messageEvent);
            }
            // case 2: this event is released when the network partition occurs
            // todo: to confirm (tricky part: do not release this event until it is scheduled)
            else if (partitionMap.get(sendingNodeId).get(receivingNodeId)) {
                id = TestingDef.RetCode.NODE_PAIR_IN_PARTITION;
                messageEvent.setExecuted();
                schedulingStrategy.remove(messageEvent);
            }

        }

        return id;
    }

    @Override
    public int offerMessageToFollower(int sendingSubnodeId, String receivingAddr, long zxid, String payload, int type) throws RemoteException {
        if (!clientInitializationDone) {
            LOG.debug("----client initialization is not done!---");
            return TestingDef.RetCode.CLIENT_INITIALIZATION_NOT_DONE;
        }
        final List<Event> predecessorEvents = new ArrayList<>();

        final Subnode sendingSubnode = subnodes.get(sendingSubnodeId);
        final int sendingNodeId = sendingSubnode.getNodeId();
//        getSubNodeByID.put(sendingSubnodeId, sendingNodeId);

        int receivingNodeId;
        synchronized (controlMonitor) {
            waitFollowerSocketAddrRegistered(receivingAddr);
            receivingNodeId = followerSocketAddressBook.indexOf(receivingAddr);
            LOG.debug("receivingNodeId: {}, {}", receivingNodeId, receivingAddr);
        }

        if (partitionMap.get(sendingNodeId).get(receivingNodeId)){
            return TestingDef.RetCode.NODE_PAIR_IN_PARTITION;
        }

        int id = generateEventId();
        final LearnerHandlerMessageEvent messageEvent = new LearnerHandlerMessageEvent(
                id, sendingSubnodeId, receivingNodeId, type, zxid, payload, learnerHandlerMessageExecutor);
        messageEvent.addAllDirectPredecessors(predecessorEvents);

        synchronized (controlMonitor) {
            LOG.debug("Leader {} is offering a proposal to follower {}: msgId = {}, " +
                    "set subnode {} to SENDING state", sendingNodeId, receivingNodeId, id, sendingSubnodeId);

            addEvent(messageEvent);
            sendingSubnode.setState(SubnodeState.SENDING);
            controlMonitor.notifyAll();

            waitMessageReleased(id, sendingNodeId);

            // normally, this event is released when scheduled except:
            // case 1: this event is released since the sending node is stopping
            if (NodeState.STOPPING.equals(nodeStates.get(sendingNodeId))) {
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
    public int offerRequestProcessorMessage(int subnodeId, SubnodeType subnodeType, long zxid, String payload) throws RemoteException {
        if (!clientInitializationDone) {
            LOG.debug("----client initialization is not done!---");
            return TestingDef.RetCode.CLIENT_INITIALIZATION_NOT_DONE;
        }
        final Subnode subnode = subnodes.get(subnodeId);
        final int nodeId = subnode.getNodeId();
//        getSubNodeByID.put(subnodeId, nodeId);

        int id = generateEventId();
        final RequestEvent requestEvent =
                new RequestEvent(id, nodeId, subnodeId, subnodeType, payload, zxid, requestProcessorExecutor);
        synchronized (controlMonitor) {
            LOG.debug("{} {} of Node {} is about to process the request ({}): msgId = {}, " +
                    "set subnode {} to SENDING state", subnodeType, subnodeId, nodeId, payload, id, subnodeId);
            addEvent(requestEvent);
            subnode.setState(SubnodeState.SENDING);
            if (SubnodeType.COMMIT_PROCESSOR.equals(subnodeType)) {
                zxidToCommitMap.put(zxid, zxidToCommitMap.getOrDefault(zxid, 0) + 1);
            }
            controlMonitor.notifyAll();
//            waitLogRequestReleased(id, nodeId);
            waitMessageReleased(id, nodeId);
            // If this message is released due to the STOPPING node state, then set the id = -1
            if (NodeState.STOPPING.equals(nodeStates.get(nodeId))) {
                LOG.debug("----------setting requestEvent executed. {}", requestEvent);
                id = TestingDef.RetCode.NODE_CRASH;
                requestEvent.setExecuted();
                schedulingStrategy.remove(requestEvent);
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
    public void releaseMessage(final MessageEvent event) {
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

    /***
     * From leader to follower
     * set sendingSubnode and receivingSubnode SYNC_PROCESSOR to PROCESSING
     */
    public void releaseMessageToFollower(final LearnerHandlerMessageEvent event) {
        messageInFlight = event.getId();
        final Subnode sendingSubnode = subnodes.get(event.getSendingSubnodeId());

        // set the sending subnode to be PROCESSING
        sendingSubnode.setState(SubnodeState.PROCESSING);

        // if in partition, then just drop it
        final int sendingNodeId = sendingSubnode.getNodeId();
        final int receivingNodeId = event.getReceivingNodeId();
        if (partitionMap.get(sendingNodeId).get(receivingNodeId)) {
            return;
        }

        // not int partition
        final int type = event.getType();
        if (MessageType.PROPOSAL == type) {
            for (final Subnode subnode : subnodeSets.get(event.getReceivingNodeId())) {
                if (subnode.getSubnodeType() == SubnodeType.SYNC_PROCESSOR
                        && SubnodeState.RECEIVING.equals(subnode.getState())) {
                    // set the receiving subnode to be PROCESSING
                    subnode.setState(SubnodeState.PROCESSING);
                    break;
                }
            }
        } else if (MessageType.COMMIT == type){
            for (final Subnode subnode : subnodeSets.get(event.getReceivingNodeId())) {
                if (subnode.getSubnodeType() == SubnodeType.COMMIT_PROCESSOR
                        && SubnodeState.RECEIVING.equals(subnode.getState())) {
                    // set the receiving subnode to be PROCESSING
                    subnode.setState(SubnodeState.PROCESSING);
                    break;
                }
            }
        }
        controlMonitor.notifyAll();
    }

    // Should be called while holding a lock on controlMonitor
    /***
     * For sync
     * set SYNC_PROCESSOR to PROCESSING
     * @param event
     */
    public void releaseRequestProcessor(final RequestEvent event) {
//        logRequestInFlight = event.getId();
        messageInFlight = event.getId();
        SubnodeType subnodeType = event.getSubnodeType();
        if (SubnodeType.SYNC_PROCESSOR.equals(subnodeType)) {
            final Long zxid = event.getZxid();
            zxidSyncedMap.put(zxid, zxidSyncedMap.getOrDefault(zxid, 0) + 1);
        }
        final Subnode syncSubnode = subnodes.get(event.getSubnodeId());
        // set the corresponding subnode to be PROCESSING
        syncSubnode.setState(SubnodeState.PROCESSING);
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
        // 2. EXECUTION
        ensemble.startNode(nodeId);
        // 3. POST_EXECUTION: wait for the state to be stable
        // the started node will call for remote service of nodeOnline(..)
        for (int id = 0; id < schedulerConfiguration.getNumNodes(); id++) {
            LOG.debug("--------nodeid: {}: phase: {}, leaderElectionState: {}",
                    id, nodePhases.get(id), leaderElectionStates.get(nodeId));
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
        }

        votes.set(nodeId, null);
        // still keep the role info before crash, this is for further property check
//        leaderElectionStates.set(nodeId, LeaderElectionState.NULL);

        // 2. EXECUTION
        ensemble.stopNode(nodeId);

        // 3. POST_EXECUTION: wait for the state to be stable
        // This has been done in step 1 (set OFFLINE)

    }

    /***
     * Called by the partition start executor
     * @return
     */
    public void startPartition(final int node1, final int node2) {
        // PRE_EXECUTION: set unstable state (set STARTING)
//        nodeStates.set(node1, NodeState.STARTING);
//        nodeStates.set(node2, NodeState.STARTING);

        // 2. EXECUTION
        LOG.debug("start partition: {} & {}", node1, node2);
        LOG.debug("before: {}, {}, {}", partitionMap.get(0), partitionMap.get(1), partitionMap.get(2));
        partitionMap.get(node1).set(node2, true);
        partitionMap.get(node2).set(node1, true);
        LOG.debug("after: {}, {}, {}", partitionMap.get(0), partitionMap.get(1), partitionMap.get(2));


        // wait for the state to be stable (set ONLINE)
//        nodeStates.set(node1, NodeState.ONLINE);
//        nodeStates.set(node2, NodeState.ONLINE);

        controlMonitor.notifyAll();
    }

    /***
     * Called by the partition stop executor
     * @return
     */
    public void stopPartition(final int node1, final int node2) {
        // 1. PRE_EXECUTION: set unstable state (set STARTING)
//        nodeStates.set(node1, NodeState.STARTING);
//        nodeStates.set(node2, NodeState.STARTING);

        // 2. EXECUTION
        partitionMap.get(node1).set(node2, false);
        partitionMap.get(node2).set(node1, false);

        // wait for the state to be stable (set ONLINE)
//        nodeStates.set(node1, NodeState.ONLINE);
//        nodeStates.set(node2, NodeState.ONLINE);

        controlMonitor.notifyAll();
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
                executionWriter.write(
                        "\n---Update Node " + nodeId + "'s lastProcessedZxid: 0x" + Long.toHexString(lastProcessedZxid));
//                for (int i = 0; i < schedulerConfiguration.getNumNodes(); i++) {
//                    executionWriter.write(" # " + Long.toHexString(lastProcessedZxids.get(i)));
//                }
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
        // TODO: Leader needs to collect quorum. Here we suppose 1 learnerHanlder is enough
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
    private void waitAllNodesVoted() {
        wait(allNodesVoted, 1000L);
    }
    private void waitAllNodesVoted(final long timeout) {
        wait(allNodesVoted, timeout);
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
    public void waitQuorumToCommit(final RequestEvent event) {
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