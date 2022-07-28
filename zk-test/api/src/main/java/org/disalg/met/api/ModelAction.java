package org.disalg.met.api;

public enum ModelAction {
    // external events
    NodeCrash,
    NodeStart,
    PartitionStart,
    PartitionRecover,

    ClientGetData,

    // election & discovery
    ElectionAndDiscovery,

    // sync
    LeaderSyncFollower,
    FollowerProcessSyncMessage,
    FollowerProcessPROPOSALInSync,
    FollowerProcessCOMMITInSync,
    FollowerProcessNEWLEADER,
    LeaderProcessACKLD,
    FollowerProcessUPTODATE,

    // broadcast with sub-actions
    LeaderProcessRequest, LogPROPOSAL,
    LeaderProcessACK, FollowerToLeaderACK, ProcessCOMMIT,
    FollowerProcessPROPOSAL, LeaderToFollowerProposal, // follower here also needs to LogPROPOSAL
    FollowerProcessCOMMIT, LeaderToFollowerCOMMIT // follower here also needs to ProcessCOMMIT

}
