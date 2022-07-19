package org.mpisws.hitmc.api;

import org.mpisws.hitmc.api.configuration.SchedulerConfigurationException;

public interface Ensemble {

    void startNode(int node);

    void stopNode(int node);

    void configureEnsemble(String executionId) throws SchedulerConfigurationException;

    void startEnsemble();

    void stopEnsemble();
}
