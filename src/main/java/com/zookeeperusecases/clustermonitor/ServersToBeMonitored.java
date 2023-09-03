package com.zookeeperusecases.clustermonitor;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

import java.io.IOException;
import java.util.UUID;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
* PREREQUISITE:HAVE ZOOKEEPER RUNNING ON PORT 2181. ALSO RUN ZookeeperMonitor first then this class i.e ServersToBe
*
* This class is used to replicate the scenario where different hosts are creating an epheremal node on zookeeper
* We would run three instances of this ServersTo... and each would create a children in node /member like /member/Ijdsfdsf , member/cdfdsfs
*
* Then we turn on the ZookeeperMonitor class and that would create a watcher on children of /member.
* As soon as one instance goes down i.e. we shut down the instance of ServersToBeMonitored the children in /member
* would change and that would trigger the watcher in ZookeeperMonitor class.
*
* This simulates how we can create a cluster monitoring service using zookeeper
* We can do anything under the ZookeeperMonitor watcher , we can create a new instance of the shutdown instance or send a mail to the developer etc
*
*/
public class ServersToBeMonitored {

  private static String MembersNode = "/members";
  private static final Logger LOG = LoggerFactory.getLogger(ServersToBeMonitored.class);

  public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
    String id =  UUID.randomUUID().toString();
    LOG.info("my id  = "+ id);
    ZooKeeper zookeeper = new ZooKeeper("localhost:2181", 15000, null);
    var creationResponse = zookeeper.create(MembersNode+"/"+ id, id.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, null);
    LOG.info(creationResponse);
    Thread.sleep(100_000_000);
  }

}
