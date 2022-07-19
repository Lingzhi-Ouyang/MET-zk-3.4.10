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

    private final TestingService testingService;

    private final int clientId;

    volatile boolean ready;

    volatile boolean stop;

    private static int count = 0;

    private String serverList = "127.0.0.1:4002,127.0.0.1:4001,127.0.0.1:4000";

    private ZooKeeperClient zooKeeperClient;
    LinkedBlockingQueue<ClientRequestEvent> requestQueue = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<String> responseQueue = new LinkedBlockingQueue<>();

    public ClientProxy(final TestingService testingService){
        this.ready = false;
        this.stop = true;
        this.testingService = testingService;
        this.requestQueue.clear();
        this.responseQueue.clear();
        this.clientId = count;
        this.setName("ZooKeeperClient-" + clientId);
        this.count++;
    }

    public ClientProxy(final TestingService testingService, final int clientId, final String serverList){
        this.ready = false;
        this.stop = true;

        this.testingService = testingService;
        this.serverList = serverList;
        this.requestQueue.clear();
        this.responseQueue.clear();

        this.clientId = clientId;
        this.setName("ZooKeeperClient-" + clientId);
        this.count++;
    }

    public boolean isReady() {
        return ready;
    }

    public boolean isStop() {
        return stop;
    }

    public boolean init() {

        this.ready = false;

        int retry = 5;
        while (retry > 0) {
            try {
                zooKeeperClient = new ZooKeeperClient(this, serverList, true);
                zooKeeperClient.getCountDownLatch().await();

                LOG.debug("----------create /test-------");
//                if (count % 2 != 0) {
//                    zooKeeperClient.create();
//                }
                zooKeeperClient.create();

                return true;
            } catch (InterruptedException | KeeperException | IOException e) {
                LOG.debug("----- caught {} during client session initialization", e.toString());
                e.printStackTrace();
                retry--;
            }
        }
        return false;
    }

    public void shutdown(){
        LOG.debug("---shutting down zookeeper client proxy thread {}", this.getName());
        this.ready = false;
        this.stop = true;
    }

    public LinkedBlockingQueue<ClientRequestEvent> getRequestQueue() {
        return requestQueue;
    }

    public LinkedBlockingQueue<String> getResponseQueue() {
        return responseQueue;
    }

    public TestingService getTestingService() {
        return testingService;
    }

    @Override
    public void run() {
        stop = false;
        while (!stop) {
            if (init()) {
                this.ready = true;
                LOG.info("Thread {} is ready", currentThread().getName());
            } else {
                LOG.info("Something wrong during Thread {} initializing ZooKeeper client.", currentThread().getName());
                return;
            }
            synchronized (testingService.getControlMonitor()) {
                testingService.getControlMonitor().notifyAll();
            }
            while (ready) {
                try {
                    ClientRequestEvent m = requestQueue.poll(3000, TimeUnit.MILLISECONDS);
                    if(m == null) continue;
                    process(m);
                } catch (InterruptedException | KeeperException e) {
                    e.printStackTrace();
                    break;
                }
            }
            LOG.info("Thread {} is not ready", currentThread().getName());
            // TODO: closeSession is time consuming. Any better idea?
            try {
                zooKeeperClient.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            this.ready = false;
            // let the proxy start new session
//            this.stop = true;
        }
        LOG.info("Thread {} is stopping", currentThread().getName());
    }

    private void process(ClientRequestEvent event) throws InterruptedException, KeeperException {
        switch (event.getType()) {
            case GET_DATA:
                String result = zooKeeperClient.getData();
//                zooKeeperClient.getDataAsync(event);
//                String result = responseQueue.poll();
//                while (result == null) {
//                    try {
//                        LOG.debug("---wait for GET_DATA result");
//                        result = responseQueue.poll(3000, TimeUnit.MILLISECONDS);
//                    } catch (Exception e) {
//                        e.printStackTrace();
////                    break;
//                    }
//                }
                LOG.debug("---done wait for GET_DATA result: {}", result);
                event.setResult(result);
                break;
            case SET_DATA:
                // always return immediately
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
