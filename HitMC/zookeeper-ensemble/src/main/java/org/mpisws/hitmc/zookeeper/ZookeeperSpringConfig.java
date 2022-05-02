package org.mpisws.hitmc.zookeeper;

import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.predicate.AllNodesSteady;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ZookeeperSpringConfig {

    @Bean
    public LeaderElectionGlobalState leaderElectionGlobalState() {
        return new LeaderElectionGlobalState();
    }

    @Bean
    public LeaderElectionVerifier leaderElectionVerifier() {
        return new LeaderElectionVerifier();
    }

    @Bean
    public ZookeeperEnsemble zookeeperEnsemble() {
        return new ZookeeperEnsemble();
    }

    @Bean
    public ZooKeeperClientGroup zooKeeperClientGroup() {
        return new ZooKeeperClientGroup();
    }

    @Bean
    public ZookeeperConfiguration zookeeperConfiguration() {
        return new ZookeeperConfiguration();
    }

    @Bean
    public TestingService testingService() {
        return new TestingService();
    }

}
