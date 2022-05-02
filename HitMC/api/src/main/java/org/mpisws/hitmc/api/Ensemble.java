package org.mpisws.hitmc.api;

import org.mpisws.hitmc.api.configuration.SchedulerConfigurationException;

public interface Ensemble {

    void startNode(int node);

    void stopNode(int node);

    void configureEnsemble(int executionId) throws SchedulerConfigurationException;

    void startEnsemble();

    void stopEnsemble();
}
