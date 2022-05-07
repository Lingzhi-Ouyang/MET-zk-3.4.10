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

    private static final int SESSION_TIME_OUT = 1000000;
    private static final String CONNECT_STRING = "127.0.0.1:4002";
    private static final String ZNODE_PATH = "/test";
    private static final String INITIAL_VAL = "0";

    private static CountDownLatch countDownLatch;

    private boolean isSyncConnected;

    private Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            LOG.debug(">>>>>> event.getPath: " + event.getPath() +
                    " >>>>>> event.getType: " + event.getType() +
                    " >>>>>> event.getState: " + event.getState());
            if (Watcher.Event.KeeperState.SyncConnected.equals(event.getState())){
                LOG.debug("connected successfully!");
                countDownLatch.countDown();
                isSyncConnected = true;
            } else {
                try {
                    LOG.debug("非SyncConnected状态！将断开连接！");
                    countDownLatch = new CountDownLatch(1);
                    isSyncConnected = false;
                    zk.close();
                } catch (InterruptedException e) {
                    LOG.debug("----caught InterruptedException: {} ", e.toString());
                    e.printStackTrace();
                }
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
        countDownLatch = new CountDownLatch(1);
        isSyncConnected = false;
        zk = new ZooKeeper(CONNECT_STRING, SESSION_TIME_OUT, watcher);
    }

    public boolean existTestPath() throws KeeperException, InterruptedException {
        return zk.exists(ZNODE_PATH, watcher) != null;
    }

    public String create(boolean deleteFlag) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(ZNODE_PATH, watcher);
        if (stat != null && deleteFlag) {
            zk.delete(ZNODE_PATH, -1);
        }
        String createdPath = zk.create(ZNODE_PATH, INITIAL_VAL.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        LOG.debug("CREATE PATH {}: {}", createdPath, INITIAL_VAL);
        return createdPath;
    }

    public List<String> ls() throws KeeperException, InterruptedException {
        List<String> data = zk.getChildren(ZNODE_PATH, null);
        LOG.debug("ls {}: {}", ZNODE_PATH, data);
        return data;
    }

    public String getData() throws KeeperException, InterruptedException {
        byte[] data = zk.getData(ZNODE_PATH, false, null);
        String result = new String(data);
        LOG.debug("after GET data of {}: {}", ZNODE_PATH, result);
        return result;
    }

    public void setData(String val) throws KeeperException, InterruptedException {
        int version = -1;
        zk.setData(ZNODE_PATH, val.getBytes(), version);
        LOG.debug("after Set data of {}: {}", ZNODE_PATH, val);
    }

}
