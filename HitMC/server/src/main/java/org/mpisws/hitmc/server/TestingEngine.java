package org.mpisws.hitmc.server;

import org.apache.zookeeper.KeeperException;
import org.mpisws.hitmc.api.Ensemble;
import org.mpisws.hitmc.api.TestingRemoteService;
import org.mpisws.hitmc.api.configuration.SchedulerConfiguration;
import org.mpisws.hitmc.api.configuration.SchedulerConfigurationException;
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

public class TestingEngine {

    private static final Logger LOG = LoggerFactory.getLogger(TestingEngine.class);

}
