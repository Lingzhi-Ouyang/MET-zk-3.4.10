package org.mpisws.hitmc.server;

import org.apache.zookeeper.*;
import org.mpisws.hitmc.api.*;
import org.mpisws.hitmc.api.configuration.SchedulerConfiguration;
import org.mpisws.hitmc.api.configuration.SchedulerConfigurationException;
import org.mpisws.hitmc.api.state.ClientRequestType;
import org.mpisws.hitmc.api.state.LeaderElectionState;
import org.mpisws.hitmc.api.state.Vote;
import org.mpisws.hitmc.server.checker.LeaderElectionVerifier;
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
import java.util.concurrent.atomic.AtomicInteger;

public class TestingService implements TestingRemoteService {

    private static final Logger LOG = LoggerFactory.getLogger(TestingService.class);

    @Autowired
    private SchedulerConfiguration schedulerConfiguration;

    @Autowired
    private Ensemble ensemble;

    private ZKClient zkClient;

    private SchedulingStrategy schedulingStrategy;

    // External event executors
    private NodeStartExecutor nodeStartExecutor;
    private NodeCrashExecutor nodeCrashExecutor;
    private ClientRequestExecutor clientRequestExecutor;
    private PartitionStartExecutor partitionStartExecutor;
    private PartitionStopExecutor partitionStopExecutor;

    // Internal event executors
    private MessageExecutor messageExecutor; // for election
    private LogRequestExecutor logRequestExecutor; // for logging
    private LearnerHandlerMessageExecutor learnerHandlerMessageExecutor; // for learnerHandler-follower

    private Statistics statistics;

    private LeaderElectionVerifier leaderElectionVerifier;

    private FileWriter statisticsWriter;
    private FileWriter executionWriter;
    //private FileWriter vectorClockWriter;

    private final Object controlMonitor = new Object();

    // For indication of client initialization
    private boolean isClientInitializationDone = false;

    // For network partition
    private List<List<Boolean>> partitionMap = new ArrayList<>();

    private final List<NodeState> nodeStates = new ArrayList<>();
    private final List<Subnode> subnodes = new ArrayList<>();
    private final List<Set<Subnode>> subnodeSets = new ArrayList<>();

    private final List<String> followerSocketAddressBook = new ArrayList<>();

    private final List<NodeStateForClientRequest> nodeStateForClientRequests = new ArrayList<>();

    // properties
    private final List<Long> nodeProperties = new ArrayList<>();

    private final AtomicInteger eventIdGenerator = new AtomicInteger();

    // for event dependency
    private final Map<Integer, MessageEvent> messageEventMap = new HashMap<>();
//    private final Map<Integer, LearnerHandlerMessageEvent> followerMessageEventMap = new HashMap<>();

    private final List<NodeStartEvent> lastNodeStartEvents = new ArrayList<>();
    private final List<Boolean> firstMessage = new ArrayList<>();

    private int messageInFlight;

    private int logRequestInFlight;

    private final List<Vote> votes = new ArrayList<>();
    private final List<LeaderElectionState> leaderElectionStates = new ArrayList<>();

    //private int[][] vectorClock;
    //private Map <Event, List> vectorClockEvent;

//    private Map <Integer, Integer> getSubNodeByID;

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

    public List<NodeStateForClientRequest> getNodeStateForClientRequests() {
        return nodeStateForClientRequests;
    }

    public NodeStartExecutor getNodeStartExecutor() {
        return nodeStartExecutor;
    }

    public NodeCrashExecutor getNodeCrashExecutor() {
        return nodeCrashExecutor;
    }

    public ClientRequestExecutor getClientRequestExecutor() {
        return clientRequestExecutor;
    }

    public Ensemble getEnsemble() {
        return ensemble;
    }

    public ZKClient getZkClient() { return zkClient; }

    public SchedulerConfiguration getSchedulerConfiguration() {
        return schedulerConfiguration;
    }

    public void loadConfig(final String[] args) throws SchedulerConfigurationException {
        schedulerConfiguration.load(args);
    }

    /***
     * The core process the scheduler
     * @throws SchedulerConfigurationException
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void start() throws SchedulerConfigurationException, IOException, InterruptedException, KeeperException {
        LOG.debug("Starting the scheduler");
        long startTime = System.currentTimeMillis();
        initRemote();

        for (int executionId = 1; executionId <= schedulerConfiguration.getNumExecutions(); ++executionId) {
            ensemble.configureEnsemble(executionId);
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

            // Start the timer for recoding statistics
            statistics.startTimer();

            // PRE: first election
            totalExecuted = scheduleFirstElection(totalExecuted);

            // 1. trigger DIFF
            configureClientRequests();
            totalExecuted = triggerDiff(totalExecuted);

            // 2. phantom read

            zkClient.deregister();
            ensemble.stopEnsemble();

            executionWriter.close();
            statisticsWriter.close();
        }
        LOG.debug("total time: {}" , (System.currentTimeMillis() - startTime));
    }

    private void initRemote() {
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
        clientRequestExecutor = new ClientRequestExecutor(this);
        partitionStartExecutor = new PartitionStartExecutor(this);
        partitionStopExecutor = new PartitionStopExecutor(this);

        // Configure external event executors
        messageExecutor = new MessageExecutor(this);
        logRequestExecutor = new LogRequestExecutor(this);
        learnerHandlerMessageExecutor = new LearnerHandlerMessageExecutor(this);

        leaderElectionVerifier = new LeaderElectionVerifier(this, statistics);

        // Configure nodes and subnodes
        nodeProperties.clear();
        nodeStates.clear();
        subnodeSets.clear();
        subnodes.clear();
        nodeStateForClientRequests.clear();
        followerSocketAddressBook.clear();

        for (int i = 0 ; i < schedulerConfiguration.getNumNodes(); i++) {
            nodeStates.add(NodeState.STARTING);
            subnodeSets.add(new HashSet<Subnode>());
            nodeProperties.add(0L);
            nodeStateForClientRequests.add(NodeStateForClientRequest.SET_DONE);
            followerSocketAddressBook.add(null);
        }

        // configure network partion info
        partitionMap.clear();
        partitionMap.addAll(Collections.nCopies(schedulerConfiguration.getNumNodes(),
                new ArrayList<>(Collections.nCopies(schedulerConfiguration.getNumNodes(), false))));

        //vectorClock = new int[][]{{0, 0, 0}, {0, 0, 0}, {0, 0, 0}};
        //vectorClockEvent = new HashMap<>();

//        getSubNodeByID = new HashMap<>();

        eventIdGenerator.set(0);
        messageEventMap.clear();
        messageInFlight = 0;
        logRequestInFlight = 0;

        firstMessage.clear();
        firstMessage.addAll(Collections.<Boolean>nCopies(schedulerConfiguration.getNumNodes(), null));

        votes.clear();
        votes.addAll(Collections.<Vote>nCopies(schedulerConfiguration.getNumNodes(), null));

        leaderElectionStates.clear();
        leaderElectionStates.addAll(Collections.nCopies(schedulerConfiguration.getNumNodes(), LeaderElectionState.NULL));

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
     * @throws InterruptedException
     * @throws KeeperException
     * @throws IOException
     */
    private void configureClientRequests() throws InterruptedException, KeeperException, IOException {
        // Initialize zkClients
        // Attention: do not intercept any event during the initialization process
        LOG.debug("------------------start the client session initialization------------------");
        zkClient = new ZKClient();
        zkClient.init();
        zkClient.start();
        LOG.debug("------------------finish the client session initialization------------------\n");
        isClientInitializationDone = true; // at this time no client request is produced

//        // provide initial client requests
//        final ClientRequestEvent getDataEvent = new ClientRequestEvent(generateEventId(),
//                ClientRequestType.GET_DATA, clientRequestExecutor);
//        addEvent(getDataEvent);
//        final ClientRequestEvent setDataEvent = new ClientRequestEvent(generateEventId(),
//                ClientRequestType.SET_DATA, clientRequestExecutor);
//        addEvent(setDataEvent);

        // Reconfigure executors
        nodeStartExecutor = new NodeStartExecutor(this, schedulerConfiguration.getNumRebootsAfterElection());
        nodeCrashExecutor = new NodeCrashExecutor(this, schedulerConfiguration.getNumCrashesAfterElection());
//
//        // Generate node crash events
//        if (schedulerConfiguration.getNumCrashes() > 0) {
//            for (int i = 0; i < schedulerConfiguration.getNumNodes(); i++) {
//                final NodeCrashEvent nodeCrashEvent = new NodeCrashEvent(generateEventId(), i, nodeCrashExecutor);
//                schedulingStrategy.add(nodeCrashEvent);
//            }
//        }
    }

    /***
     * The schedule process from the beginning to the end of the first round of election
     * @param totalExecuted the number of previous executed events
     * @return the number of executed events
     */
    private int scheduleFirstElection(int totalExecuted) {
        try{
            synchronized (controlMonitor) {
                waitAllNodesSteady();
                while (schedulingStrategy.hasNextEvent() && totalExecuted < 100) {
                    long begintime = System.currentTimeMillis();
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                    final Event event = schedulingStrategy.nextEvent();
                    if (event.execute()) {
                        ++totalExecuted;
                        recordProperties(totalExecuted, begintime, event);
                    }
                }
                // wait whenever an election ends
                waitAllNodesVoted();
            }
            statistics.endTimer();
            // check election results
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
                waitAllNodesSteadyForClientMutation();
                LOG.debug("All Nodes steady for client requests");
                for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); nodeId++) {
                    executionWriter.write(nodeProperties.get(nodeId).toString() + " # ");
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
                            executionWriter.write(nodeProperties.get(nodeId).toString() + " # ");
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
     * the steps to trigger DIFF
     */
    private int triggerDiff(int totalExecuted) {
        try {
            synchronized (controlMonitor) {
                // last event executor has waited for all nodes steady
                // waitAllNodesSteady();

                // PRE
                // >> client set request
                long startTime = System.currentTimeMillis();
                Event event = new ClientRequestEvent(generateEventId(),
                        ClientRequestType.SET_DATA, clientRequestExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
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

                // TODO: check quorum nodes has updated lastProcessedZxid whenever a client mutation is done
//                waitQuorumZxidUpdated();

                // make DIFF: client set >> leader log >> leader restart >> re-election
                // Step 1. client request SET_DATA
                startTime = System.currentTimeMillis();
                event = new ClientRequestEvent(generateEventId(),
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
                while (!(event instanceof LogRequestEvent)){
                    LOG.debug("-------need LogRequestEvent! get event: {}", event);
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
                // >> This is the only enabled event now
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
                // check election results
                leaderElectionVerifier.verify();
                // TODO: check the original leader becomes the new leader again. o.w. repeat Step 4-5
                // TODO: check DIFF

                // Step POST. client request SET_DATA
                startTime = System.currentTimeMillis();
                event = new ClientRequestEvent(generateEventId(),
                        ClientRequestType.SET_DATA, clientRequestExecutor);
                LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted + 1);
                LOG.debug("prepare to execute event: {}", event);
                if (event.execute()) {
                    ++totalExecuted;
                    recordProperties(totalExecuted, startTime, event);
                }

                while (schedulingStrategy.hasNextEvent() && totalExecuted < 100) {
                    startTime = System.currentTimeMillis();
                    event = schedulingStrategy.nextEvent();
                    LOG.debug("\n\n\n\n\n---------------------------Step: {}--------------------------", totalExecuted);
                    LOG.debug("prepare to execute event: {}", event.toString());
                    if (event.execute()) {
                        ++totalExecuted;
                        recordProperties(totalExecuted, startTime, event);
                    }
                }
            }
            statistics.endTimer();
            // TODO: property check : S0 writes PROPOSAL T(zxid=1) to the datafile. Neither S1 or S2 receives PROPOSAL T(zxid=1).
//            leaderElectionVerifier.verify();
            statistics.reportTotalExecutedEvents(totalExecuted);
            statisticsWriter.write(statistics.toString() + "\n\n");
            LOG.info(statistics.toString() + "\n\n\n\n\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalExecuted;
    }

    public void addEvent(final Event event) {
        schedulingStrategy.add(event);

        /*try {
            vectorClockWriter.write("Added " + event.toString() + "\n");
        }
        catch (final IOException e) {
            LOG.debug("IO exception", e);
        }
        if (event instanceof MessageEvent)
        {
            LOG.debug(getSubNodeByID.get(((MessageEvent) event).getSendingSubnodeId()) + " " + ((MessageEvent) event).getSendingSubnodeId());
            vectorClock[getSubNodeByID.get(((MessageEvent) event).getSendingSubnodeId())][getSubNodeByID.get(((MessageEvent) event).getSendingSubnodeId())]++;
            List <Integer> tmp = new ArrayList<Integer>();
            tmp.add(vectorClock[getSubNodeByID.get(((MessageEvent) event).getSendingSubnodeId())][0]);
            tmp.add(vectorClock[getSubNodeByID.get(((MessageEvent) event).getSendingSubnodeId())][1]);
            tmp.add(vectorClock[getSubNodeByID.get(((MessageEvent) event).getSendingSubnodeId())][2]);
            vectorClockEvent.put(event, tmp);
            try {
                vectorClockWriter.write(vectorClock[getSubNodeByID.get(((MessageEvent) event).getSendingSubnodeId())][0] + " " +
                        vectorClock[getSubNodeByID.get(((MessageEvent) event).getSendingSubnodeId())][1] + " " + vectorClock[getSubNodeByID.get(((MessageEvent) event).getSendingSubnodeId())][2] + "\n");
            }
            catch (final IOException e) {
                LOG.debug("IO exception", e);
            }
        }*/
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
            }
            // case 2: this event is released when the network partition occurs
            // todo: to confirm (tricky part: do not release this event until it is scheduled)
            else if (partitionMap.get(sendingNodeId).get(receivingNodeId)) {
                id = TestingDef.RetCode.NODE_PAIR_IN_PARTITION;
                messageEvent.setExecuted();
            }

        }

        return id;
    }

    @Override
    public int offerMessageToFollower(int sendingSubnodeId, String receivingAddr, String payload, int type) throws RemoteException {
        if (!isClientInitializationDone) {
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
        final LearnerHandlerMessageEvent messageEvent = new LearnerHandlerMessageEvent(id, sendingSubnodeId, receivingNodeId, payload, learnerHandlerMessageExecutor);
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
            }
            // case 2: this event is released when the network partition occurs
            // todo: to confirm (tricky part: do not release this event until it is scheduled)
            else if (partitionMap.get(sendingNodeId).get(receivingNodeId)) {
                id = TestingDef.RetCode.NODE_PAIR_IN_PARTITION;
                messageEvent.setExecuted();
            }
        }
        return id;
    }

    @Override
    public int logRequestMessage(int syncSubnodeId, String payload, int type) throws RemoteException {
        if (!isClientInitializationDone) {
            LOG.debug("----client initialization is not done!---");
            return TestingDef.RetCode.CLIENT_INITIALIZATION_NOT_DONE;
        }
        final Subnode syncSubnode = subnodes.get(syncSubnodeId);
        final int syncNodeId = syncSubnode.getNodeId();
//        getSubNodeByID.put(syncSubnodeId, syncNodeId);

        int id = generateEventId();
        final LogRequestEvent logRequestEvent = new LogRequestEvent(id, syncNodeId, syncSubnodeId, payload, logRequestExecutor);
        synchronized (controlMonitor) {
            LOG.debug("Node {} is about to log a proposal ({}): msgId = {}, " +
                    "set subnode {} to SENDING state", syncNodeId, payload, id, syncSubnodeId);
            addEvent(logRequestEvent);
            syncSubnode.setState(SubnodeState.SENDING);
            controlMonitor.notifyAll();
//            waitLogRequestReleased(id, syncNodeId);
            waitMessageReleased(id, syncNodeId);
            // If this message is released due to the STOPPING node state, then set the id = -1
            if (NodeState.STOPPING.equals(nodeStates.get(syncNodeId))) {
                id = TestingDef.RetCode.NODE_CRASH;
                logRequestEvent.setExecuted();
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
        for (final Subnode subnode : subnodeSets.get(event.getReceivingNodeId())) {
            // ATTENTION: this is for sync
            if (subnode.getSubnodeType() == SubnodeType.SYNC_PROCESSOR
                    && SubnodeState.RECEIVING.equals(subnode.getState())) {
                // set the receiving subnode to be PROCESSING
                subnode.setState(SubnodeState.PROCESSING);
                break;
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
    public void releaseLogRequest(final LogRequestEvent event) {
//        logRequestInFlight = event.getId();
        messageInFlight = event.getId();
        final Subnode syncSubnode = subnodes.get(event.getSyncSubnodeId());
        // set the corresponding subnode to be PROCESSING
        syncSubnode.setState(SubnodeState.PROCESSING);
        controlMonitor.notifyAll();
    }

    /***
     * The executor of client requests
     * @param event
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void releaseClientRequest(final ClientRequestEvent event) {
        switch (event.getType()) {
            case GET_DATA:
                // TODO: this method should modify related states
//                for (int i = 0 ; i < schedulerConfiguration.getNumNodes(); i++) {
//                    nodeStateForClientRequests.set(i, NodeStateForClientRequest.SET_PROCESSING);
//                }
                zkClient.getRequestQueue().offer(event);
                // notifyAll() should be called after related states have been changed
                controlMonitor.notifyAll();

                /***
                 * use responseQueue for acquiring the result
                 */
//                while(true){
//                    try {
//                        ClientRequestEvent m = responseQueue.poll(3000, TimeUnit.MILLISECONDS);
//                        if (m == null) {
//                            Thread.sleep(500);
//                            continue;
//                        }
//                        break;
//                    } catch (InterruptedException e){
//                        e.printStackTrace();
//                        break;
//                    }
//                }
                break;
            case SET_DATA:
                for (int i = 0 ; i < schedulerConfiguration.getNumNodes(); i++) {
                    nodeStateForClientRequests.set(i, NodeStateForClientRequest.SET_PROCESSING);
                }

                // TODO: This should set the leader learnerHandlerSender / syncProcessor into PROCESSING state
                // TODO: what if leader does not exist?

                String data = String.valueOf(event.getId());
                event.setData(data);
                zkClient.getRequestQueue().offer(event);
                // notifyAll() should be called after related states have been changed
                controlMonitor.notifyAll();
                break;
        }
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
        nodeStateForClientRequests.set(nodeId, NodeStateForClientRequest.SET_DONE);
        votes.set(nodeId, null);
        leaderElectionStates.set(nodeId, LeaderElectionState.LOOKING);
        // 2. EXECUTION
        ensemble.startNode(nodeId);
        // 3. POST_EXECUTION: wait for the state to be stable
        // the started node will call for remote service of nodeOnline(..)
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

        // If hasSending == true, the node has been set OFFLINE when the last intercepted subnode is shutdown
        // o.w. set OFFLINE here anyway.
        nodeStates.set(nodeId, NodeState.OFFLINE);
        nodeStateForClientRequests.set(nodeId, NodeStateForClientRequest.SET_DONE);

        votes.set(nodeId, null);
        leaderElectionStates.set(nodeId, LeaderElectionState.NULL);

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
        nodeStates.set(node1, NodeState.STARTING);
        nodeStates.set(node2, NodeState.STARTING);

        // 2. EXECUTION
        partitionMap.get(node1).set(node2, true);
        partitionMap.get(node2).set(node1, true);

        // wait for the state to be stable (set ONLINE)
        nodeStates.set(node1, NodeState.ONLINE);
        nodeStates.set(node2, NodeState.ONLINE);

        controlMonitor.notifyAll();
    }

    /***
     * Called by the partition stop executor
     * @return
     */
    public void stopPartition(final int node1, final int node2) {
        // 1. PRE_EXECUTION: set unstable state (set STARTING)
        nodeStates.set(node1, NodeState.STARTING);
        nodeStates.set(node2, NodeState.STARTING);

        // 2. EXECUTION
        partitionMap.get(node1).set(node2, false);
        partitionMap.get(node2).set(node1, false);

        // wait for the state to be stable (set ONLINE)
        nodeStates.set(node1, NodeState.ONLINE);
        nodeStates.set(node2, NodeState.ONLINE);

        controlMonitor.notifyAll();
    }

    public int generateEventId() {
        return eventIdGenerator.incrementAndGet();
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
            try {
                LOG.debug("Writing execution file------Node {} state: {}", nodeId, state);
                executionWriter.write("\nNode " + nodeId + " state: " + state + '\n');
            } catch (final IOException e) {
                LOG.debug("IO exception", e);
            }
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
    public void updateProperties(int nodeId, long lastProcessedZxid) throws RemoteException{
        // TODO: this method is also called during initialization. Is it okay?
//        nodeProperties.set(nodeId, lastProcessedZxid);
//        try {
//                LOG.debug("Writing execution file------Node {} lastProcessedZxid: {}", nodeId, lastProcessedZxid);
//                executionWriter.write("\nnodeId: " + nodeId + " lastProcessedZxid: " + lastProcessedZxid + " ||| ");
//                for (int i = 0; i < schedulerConfiguration.getNumNodes(); i++) {
//                    executionWriter.write(nodeProperties.get(i).toString() + " | ");
//                }
//            } catch (final IOException e) {
//                LOG.debug("IO exception", e);
//        }
        synchronized (controlMonitor) {
//            LOG.debug("-------begin updateProperties, node id: {}, lastProcessedZxid: {}", nodeId, lastProcessedZxid);
            nodeProperties.set(nodeId, lastProcessedZxid);
            nodeStateForClientRequests.set(nodeId, NodeStateForClientRequest.SET_DONE);
            try {
                executionWriter.write("\nnodeId: " + nodeId + " lastProcessedZxid: " + lastProcessedZxid);
                for (int i = 0; i < schedulerConfiguration.getNumNodes(); i++) {
                    executionWriter.write("|||" + nodeProperties.get(i).toString());
                }
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
        executionWriter.write("\nlastProcessedZxid: ");
        // Property verification
        for (int nodeId = 0; nodeId < schedulerConfiguration.getNumNodes(); nodeId++) {
            executionWriter.write(nodeProperties.get(nodeId).toString() + " # ");
        }
        executionWriter.write("\ntime/ms: " + (System.currentTimeMillis() - startTime) + "\n");
        executionWriter.flush();
    }

    /***
     * The following predicates are general to some type of events.
     */

    private final WaitPredicate allNodesSteady = new AllNodesSteady(this);

    // Should be called while holding a lock on controlMonitor
    public void waitAllNodesSteady() {
        wait(allNodesSteady, 0L);
    }

    private final WaitPredicate allNodesVoted = new AllNodesVoted(this);
    // Should be called while holding a lock on controlMonitor
    private void waitAllNodesVoted() {
        wait(allNodesVoted, 1000L);
    }
    private void waitAllNodesVoted(final long timeout) {
        wait(allNodesVoted, timeout);
    }

    private final WaitPredicate allNodesSteadyForClientMutation = new AllNodesSteadyForClientMutation(this);
    // Should be called while holding a lock on controlMonitor
    public void waitAllNodesSteadyForClientMutation() {
        wait(allNodesSteadyForClientMutation, 0L);
    }

    private final WaitPredicate allNodesLogSyncSteady = new AllNodesLogSyncSteady(this);
    public void waitAllNodesLogSyncSteady() {
        wait(allNodesLogSyncSteady, 0L);
    }

    private final WaitPredicate allNodesSteadyAfterClientRequest = new AllNodesSteadyAfterClientRequest(this);
    public void waitAllNodesSteadyAfterClientRequest() {wait(allNodesSteadyAfterClientRequest, 0L);}

    /**
     * The following predicates are specific to an event.
     */

    // Should be called while holding a lock on controlMonitor
    private void waitResponseForClientRequest(ClientRequestEvent event) {
        final WaitPredicate responseForClientRequest = new ResponseForClientRequest(event);
        wait(responseForClientRequest, 0L);
    }

    private void waitFirstMessageOffered(final int nodeId) {
        final WaitPredicate firstMessageOffered = new FirstMessageOffered(this, nodeId);
        wait(firstMessageOffered, 0L);
    }

    private void waitNewMessageOffered() {
        final WaitPredicate newMessageOffered = new NewMessageOffered(this);
        wait(newMessageOffered, 0L);
    }

    private void waitMessageReleased(final int msgId, final int sendingNodeId) {
        final WaitPredicate messageReleased = new MessageReleased(this, msgId, sendingNodeId);
        wait(messageReleased, 0L);
    }

    private void waitLogRequestReleased(final int msgId, final int syncNodeId) {
        final WaitPredicate logRequestReleased = new LogRequestReleased(this, msgId, syncNodeId);
        wait(logRequestReleased, 0L);
    }

    private void waitFollowerSocketAddrRegistered(final String addr) {
        final WaitPredicate followerSocketAddrRegistered = new FollowerSocketAddrRegistered(this, addr);
        wait(followerSocketAddrRegistered, 0L);
    }


    //------------------------

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