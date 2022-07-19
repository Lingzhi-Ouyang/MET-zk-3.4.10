package org.mpisws.hitmc.api.configuration;

public interface SchedulerConfigurationPostLoadListener {

    void postLoadCallback() throws SchedulerConfigurationException;

}
