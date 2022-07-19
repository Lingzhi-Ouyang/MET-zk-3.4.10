package org.apache.zookeeper.server.quorum;

import org.mpisws.hitmc.api.TestingRemoteService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashSet;
import java.util.Set;

public aspect FollowerAspect {
    private static final Logger LOG = LoggerFactory.getLogger(FollowerAspect.class);

}
