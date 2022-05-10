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

    volatile boolean ready;

    volatile boolean stop;

    private static int count = 0;

    private ZooKeeperClient zooKeeperClient;
    LinkedBlockingQueue<ClientRequestEvent> requestQueue = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<ClientRequestEvent> responseQueue = new LinkedBlockingQueue<>();

    public ClientProxy(final TestingService testingService){
        this.ready = false;
        this.stop = true;
        this.testingService = testingService;
        this.requestQueue.clear();
        this.responseQueue.clear();
    }

    public boolean isReady() {
        return ready;
    }

    public boolean isStop() {
        return stop;
    }

    public boolean init() {
        count++;
        this.setName("ZooKeeperClient-" + count);
        this.ready = false;

        int retry = 5;
        while (retry > 0) {
            try {
                zooKeeperClient = new ZooKeeperClient();
                zooKeeperClient.getCountDownLatch().await();

                LOG.debug("----------create /test-------");
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
        LOG.debug("---shutting down zookeeper client proxy");
        this.ready = false;
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
            LOG.info("Thread {} is stopping", currentThread().getName());
            this.ready = false;
            this.stop = true;
        }
    }

//    @Override
//    public void run() {
//        boolean deleteFlag = count == 1;
//        if (init(deleteFlag)) {
//            this.ready = true;
//            LOG.info("Thread {} is ready", currentThread().getName());
//        } else {
//            LOG.info("Something wrong during Thread {} initializing ZooKeeper client.", currentThread().getName());
//            return;
//        }
//        synchronized (testingService.getControlMonitor()) {
//            testingService.getControlMonitor().notifyAll();
//        }
//        while (ready) {
//            try {
//                ClientRequestEvent m = requestQueue.poll(3000, TimeUnit.MILLISECONDS);
//                if(m == null) continue;
//                process(m);
//            } catch (InterruptedException | KeeperException e) {
//                e.printStackTrace();
//                break;
//            }
//        }
//        LOG.info("Thread {} is stopping", currentThread().getName());
//        this.ready = false;
//    }

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
