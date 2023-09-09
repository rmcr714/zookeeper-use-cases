package com.zookeeperusecases.leaderelection.keeper;

import static com.zookeeperusecases.leaderelection.common.LeaderElectionConstants.ELECTION_NAMESPACE;
import static com.zookeeperusecases.leaderelection.common.LeaderElectionConstants.SESSION_TIMEOUT;
import static com.zookeeperusecases.leaderelection.common.LeaderElectionConstants.ZOOKEEPER_ADDRESS;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.StringUtils;
import org.apache.zookeeper.data.Stat;

public class LeaderElection implements Watcher {

  ZooKeeper zooKeeper;

  @Override
  public void process(WatchedEvent event) {
    switch (event.getType()) {
      case None:
        if (event.getState() == Event.KeeperState.SyncConnected) {
          System.out.println("Successfully connected to Zookeeper");
        } else {
          synchronized (zooKeeper) {
            System.out.println("Disconnected from Zookeeper event");
            zooKeeper.notifyAll();
          }
        }
        break;
      case NodeDeleted:
        try {
          reelectLeader(event);
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (KeeperException e) {
          e.printStackTrace();
        }
        break;
      case NodeCreated:
        try {
          volunteerForLeadership();
        } catch (InterruptedException e) {
        } catch (KeeperException e) {
        }
        break;
      case NodeDataChanged:
        System.out.println("Leader updated progress of task");
        break;
    }
  }

  public void run() throws InterruptedException {
    synchronized (zooKeeper) {
      zooKeeper.wait();
    }
  }

  public void close() throws InterruptedException {
    zooKeeper.close();
  }

  public void connect() throws IOException {
    this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
  }

  public void volunteerForLeadership() throws KeeperException, InterruptedException {
    String znodePrefix = ELECTION_NAMESPACE + "/c_";
    Stat stat = zooKeeper.exists(ELECTION_NAMESPACE, false);
    if (stat == null)
      zooKeeper.create(ELECTION_NAMESPACE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);
    String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    System.out.println("Node data for the current host is "+znodeFullPath);
    Nodes.INSTANCE.addNode(znodeFullPath.replace(ELECTION_NAMESPACE + "/", ""));
  }

  public void electLeader() throws KeeperException, InterruptedException {
    Stat nodeStat = null;
    List<String> nodes = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
    Collections.sort(nodes);

    if (StringUtils.isEmpty(Nodes.INSTANCE.getCurrentLeader())) {
      String currentNode = nodes.get(0)
          .replace(ELECTION_NAMESPACE + "/", "");
      Nodes.INSTANCE.setCurrentLeader(currentNode);
      if (!Nodes.INSTANCE.getNodes().contains(currentNode)) {
        Nodes.INSTANCE.addNode(currentNode);
      } else {
        System.out.println("Current Box is leader with path "+currentNode);
        Nodes.INSTANCE.setIsCurrentBoxLeader(currentNode);
      }
    }
    if (Nodes.INSTANCE.getCurrentLeader() == nodes.get(0)) {
      System.out.println("Leader is " + Nodes.INSTANCE.getCurrentLeader());
    }
    zooKeeper.exists(ELECTION_NAMESPACE + "/" + Nodes.INSTANCE.getCurrentLeader(),
        this);
  }

  public void reelectLeader(WatchedEvent watchedEvent) throws KeeperException, InterruptedException {
    Stat nodeStat = null;

    String affectedNode = watchedEvent.getPath()
        .replace(ELECTION_NAMESPACE + "/", "");

    System.out.println("Some node crashed " + affectedNode + "...");

    if (!Nodes.INSTANCE.getCurrentLeader().equalsIgnoreCase(affectedNode)) {
      System.out.println("No change in leader, some member nodes got partitioned or crashed");
      Nodes.INSTANCE.getNodes().remove(affectedNode);
      return;
    }

    System.out.println("Aiyoo...needs re-election");

    System.out.println("Getting the volunteers or nominees...");


    List<String> nodes = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
    Collections.sort(nodes);

    if (nodes.size() == 0) {
      System.out.println("Re-election not possible. " +
          "Cluster does not any nodes to perform duty. Add nodes please.");
      return;
    }

    for (String nominee : nodes) {
      System.out.println("Nominee " + nominee);
    }


      String currentNode = nodes.get(0)
          .replace(ELECTION_NAMESPACE + "/", "");

    if (Nodes.INSTANCE.getNodes().contains(currentNode)) {
      System.out.println("Current Box is leader with path "+currentNode);
      Nodes.INSTANCE.setIsCurrentBoxLeader(currentNode);
    }

    Nodes.INSTANCE.setCurrentLeader(nodes.get(0)
        .replace(ELECTION_NAMESPACE + "/", ""));
    System.out.println("Current Leader is " + Nodes.INSTANCE.getCurrentLeader());

    System.out.println("Successful re-election. Elected " +
        Nodes.INSTANCE.getCurrentLeader());

    zooKeeper.exists(ELECTION_NAMESPACE + "/" + Nodes.INSTANCE.getCurrentLeader(),
        this);
  }
}
