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


/*
 * PREREQUISITE : Run a zookeeper server on port 2181
 * This class shows how to perform basic operations on zookeeper like create/read/update
 * This also gives an example of how to use watchers, which are essentially callback that would take
 * place if some data on which we have put watchers change
 * e.g. we have put watcher on /node then if data changes on this node the process method would run.
 *
 * Watchers are very imp as they allow to take an action in case of an event eg value of a node changes or
 * a nodes children get deleted or value changes
 * */
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

    /*
     * When we set watcher it ends after the first change. i,e watcher set at line 41 for /node would be invoked at line 49 as its data changed. Then
     * it won't get called again. So after every change we need to a get with watcher so that the next change is showed
     **/
    byte data1[] = zookeeper.getData("/node", true, stat);

    //DELETE
    zookeeper.delete("/node/child", -1);
    zookeeper.delete("/node", -1);

    Thread.sleep(10000);


  }
}
