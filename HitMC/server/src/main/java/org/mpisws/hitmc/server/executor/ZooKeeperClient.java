package org.mpisws.hitmc.server.executor;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZooKeeperClient {
    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperClient.class);

    private static final int SESSION_TIME_OUT = 100000;
    private static final String CONNECT_STRING = "127.0.0.1:4002";
    private static final String ZNODE_PATH = "/test";
    private static final String INITIAL_VAL = "0";

    private static final CountDownLatch countDownLatch = new CountDownLatch(1);

    private boolean isSyncConnected = false;

    private Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            LOG.debug(">>>>>> event.getPath: " + event.getPath() + " >>>>>> event.getType: " + event.getType());
            if (Watcher.Event.KeeperState.SyncConnected.equals(event.getState())){
                LOG.debug("connected successfully!");
                countDownLatch.countDown();
                isSyncConnected = true;
            }
        }
    };

    public CountDownLatch getCountDownLatch(){
        return countDownLatch;
    }

    public boolean syncConnected(){
        return isSyncConnected;
    }

    private ZooKeeper zk;

    public ZooKeeperClient() throws IOException, InterruptedException, KeeperException {
        isSyncConnected = false;
        zk = new ZooKeeper(CONNECT_STRING, SESSION_TIME_OUT, watcher);
    }

    public String create() throws KeeperException, InterruptedException {
        Stat stat = zk.exists(ZNODE_PATH, watcher);
        if (stat != null) {
            zk.delete(ZNODE_PATH, -1);
        }
        String createdPath = zk.create(ZNODE_PATH, INITIAL_VAL.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
//        LOG.debug("CREATE PATH {}: {}", createdPath, INITIAL_VAL);
        return createdPath;
    }

    public void ls() throws KeeperException, InterruptedException {
        List<String> data1 = zk.getChildren(ZNODE_PATH, null);
//        LOG.debug("LS {}: {}", ZNODE_PATH, data1);
    }

    public String getData() throws KeeperException, InterruptedException {
        byte[] data = zk.getData(ZNODE_PATH, false, null);
        String result = new String(data);
//        LOG.debug("after GET data of {}: {}", ZNODE_PATH, result);
        return result;
    }

    public void setData(String val) throws KeeperException, InterruptedException {
        int version = -1;
        zk.setData(ZNODE_PATH, val.getBytes(), version);
//        LOG.debug("after Set data of {}: {}", ZNODE_PATH, val);
    }

}
