package org.mpisws.hitmc.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.mpisws.hitmc.api.configuration.SchedulerConfigurationException;
import org.mpisws.hitmc.server.TestingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.IOException;

public class ZookeeperMain {

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperMain.class);

    public static void main(final String[] args) {
        final ApplicationContext applicationContext = new AnnotationConfigApplicationContext(ZookeeperSpringConfig.class);
        final TestingService testingService = applicationContext.getBean(TestingService.class);

        try {
            LOG.debug("for test......");
            testingService.loadConfig(args);
            testingService.start();

//            Thread.sleep(Long.MAX_VALUE);
            System.exit(0);
        } catch (final SchedulerConfigurationException e) {
            LOG.error("Error while reading configuration.", e);
        } catch (final IOException e) {
            LOG.error("IO exception", e);
        } catch (InterruptedException e) {
            LOG.error("Interrupted exception", e);
        } catch (KeeperException e) {
            LOG.error("Keeper exception", e);
        }
    }

}
