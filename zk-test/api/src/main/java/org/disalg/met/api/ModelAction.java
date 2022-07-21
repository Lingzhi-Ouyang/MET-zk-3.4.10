package org.disalg.met.api;

public enum ModelAction {
    // external events
    NodeCrash,
    NodeStart,
    PartitionStart,
    PartitionRecover,
    LeaderProcessRequest, LogPROPOSAL,
    ClientGetData,

    // internal events
    ElectionAndDiscovery,

    // leader
    LeaderSyncFollower,
    LeaderProcessACKLD,
    LeaderProcessACK, LeaderProcessCOMMIT, LeaderToFollowerCOMMIT,

    // follower
    FollowerProcessSyncMessage,
    FollowerProcessPROPOSALInSync,
    FollowerProcessCOMMITInSync,
    FollowerProcessNEWLEADER,
    FollowerProcessUPTODATE,
    FollowerProcessPROPOSAL,
    FollowerProcessCOMMIT

}
