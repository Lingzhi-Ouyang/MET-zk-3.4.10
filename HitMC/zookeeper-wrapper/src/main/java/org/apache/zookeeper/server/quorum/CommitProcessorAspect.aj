package org.apache.zookeeper.server.quorum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public aspect CommitProcessorAspect {
    private static final Logger LOG = LoggerFactory.getLogger(CommitProcessorAspect.class);

    private final QuorumPeerAspect quorumPeerAspect = QuorumPeerAspect.aspectOf();
}
