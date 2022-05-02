package org.mpisws.hitmc.server.executor;

import org.apache.zookeeper.KeeperException;
import org.mpisws.hitmc.server.event.ClientRequestEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ZKClient extends Thread{

    private static final Logger LOG = LoggerFactory.getLogger(ZKClient.class);

    volatile boolean stop;
    private final Object eventMonitor = new Object();
    private ZooKeeperClient zooKeeperClient;
    LinkedBlockingQueue<ClientRequestEvent> requestQueue = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<ClientRequestEvent> responseQueue = new LinkedBlockingQueue<>();

    public ZKClient(){
        this.setName("ZooKeeperClient");
        this.stop = false;
    }

    public void init() throws InterruptedException, KeeperException, IOException {
        // TODO: Create only one client
        zooKeeperClient = new ZooKeeperClient();
        zooKeeperClient.getCountDownLatch().await();
//        if(!zooKeeperClient.syncConnected()) {
//            Thread.sleep(100);
//        }
        LOG.debug("----------create /test-------");
        zooKeeperClient.create();

        requestQueue.clear();
        responseQueue.clear();
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
//        try {
//            init();
//        } catch (InterruptedException | IOException | KeeperException e) {
//            e.printStackTrace();
//        }
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
        LOG.info("ZooKeeperClient is down");
    }

    private void process(ClientRequestEvent event) throws InterruptedException, KeeperException {
        switch (event.getType()) {
            case GET_DATA:
                String result = zooKeeperClient.getData();
                LOG.debug("after getData");
                event.setResult(result);
//                    responseQueue.offer(event);
                break;
            case SET_DATA:
                zooKeeperClient.setData(event.getData());
                // TODO: will it be blocked?
                LOG.debug("after setData");
                event.setResult(event.getData());
                break;
        }
    }
}
