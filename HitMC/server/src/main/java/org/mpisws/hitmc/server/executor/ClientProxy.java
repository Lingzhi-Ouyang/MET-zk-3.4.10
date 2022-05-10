package org.mpisws.hitmc.server.executor;

import org.apache.zookeeper.*;
import org.mpisws.hitmc.server.TestingService;
import org.mpisws.hitmc.server.event.ClientRequestEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ClientProxy extends Thread{

    private static final Logger LOG = LoggerFactory.getLogger(ClientProxy.class);

    private static int count = 0;

    private final TestingService testingService;

    volatile boolean stop;

    private ZooKeeperClient zooKeeperClient;
    LinkedBlockingQueue<ClientRequestEvent> requestQueue = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<ClientRequestEvent> responseQueue = new LinkedBlockingQueue<>();

    public ClientProxy(final TestingService testingService){
        this.testingService = testingService;
        this.requestQueue.clear();
        this.responseQueue.clear();
    }

    public boolean isStop() {
        return stop;
    }

    public boolean init(boolean deleteFlag) {
        count++;
        this.setName("ZooKeeperClient-" + count);
        this.stop = true;

        int retry = 5;
        while (retry > 0) {
            try {
                zooKeeperClient = new ZooKeeperClient();
                zooKeeperClient.getCountDownLatch().await();

                LOG.debug("----------create /test-------");
                zooKeeperClient.create(deleteFlag);

                return true;
            } catch (InterruptedException | KeeperException | IOException e) {
                LOG.debug("----- caught {} during client session initialization", e.toString());
                e.printStackTrace();
                retry--;
            }
        }
        return false;
    }

    public void deregister(){
        this.stop = true;
    }

    public LinkedBlockingQueue<ClientRequestEvent> getRequestQueue() {
        return requestQueue;
    }

    public LinkedBlockingQueue<ClientRequestEvent> getResponseQueue() {
        return responseQueue;
    }

    @Override
    public void run() {
        this.stop = false;
        LOG.info("Thread {} begins to work", currentThread().getName());
        while (!stop) {
            try {
                ClientRequestEvent m = requestQueue.poll(3000, TimeUnit.MILLISECONDS);
                if(m == null) continue;
                process(m);
            } catch (InterruptedException | KeeperException e) {
                e.printStackTrace();
                break;
            }
        }
        LOG.info("Thread {} is stopping", currentThread().getName());
        this.stop = true;
    }

    private void process(ClientRequestEvent event) throws InterruptedException, KeeperException {
        switch (event.getType()) {
            case GET_DATA:
                String result = zooKeeperClient.getData();
                event.setResult(result);
                break;
            case SET_DATA:
                zooKeeperClient.setData(event.getData());
                event.setResult(event.getData());
                break;
        }
        synchronized (testingService.getControlMonitor()) {
//            responseQueue.offer(event);
            LOG.debug("-------{} result: {}", event.getType(), event.getResult());
            try {
                testingService.updateResponseForClientRequest(event);
            } catch (IOException e) {
                e.printStackTrace();
            }
            testingService.getControlMonitor().notifyAll();
        }
    }
}
