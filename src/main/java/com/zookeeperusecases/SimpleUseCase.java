package com.zookeeperusecases;

import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class SimpleUseCase {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleUseCase.class);


  public static void main(String[] args)
      throws IOException, InterruptedException, KeeperException {
    ZooKeeper zookeeper = new ZooKeeper("localhost:2181", 15000, new Watcher() {
      @Override
      public void process(WatchedEvent watchedEvent) {
        LOG.info("*********************** WATCHER CALLED *******************************");
        LOG.info("got the events for the node = " + watchedEvent.getPath());
        LOG.info("the event type = " + watchedEvent.getType());
        LOG.info("***********************************************************************");
      }
    });

    //CREATE
    zookeeper.create("/node", "data is ".getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, null);
    zookeeper.create("/node/child", "child".getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
        null);

    //READ
    Stat stat = new Stat();
    byte data[] = zookeeper.getData("/node", true, stat);
    LOG.info(new String(data));
    LOG.info(String.valueOf("version = " + stat.getVersion()));

    List<String> children = zookeeper.getChildren("/node", true);
    children.forEach(child -> LOG.info("child found " + child));

    //UPDATE
    zookeeper.setData("/node", "node data changed".getBytes(),
        -1);//Version -1 means it updates the latest version

    //When we set watcher it ends after the first change. i,e watcher set at line 41 for /node would be invoked at line 49 as its data changed. Then
    //it won't get called again. So after every change we need to a get with watcher so that the next change is showed
    byte data1[] = zookeeper.getData("/node", true, stat);

    //DELETE
    zookeeper.delete("/node/child", -1);
    zookeeper.delete("/node", -1);

    Thread.sleep(10000);


  }
}
