package com.zookeeperusecases.leaderelection;

import com.zookeeperusecases.leaderelection.keeper.LeaderElection;
import java.io.IOException;
import org.apache.zookeeper.KeeperException;

public class LeaderElectionMain {
  public static void main(String[] args)
      throws IOException, KeeperException, InterruptedException, KeeperException {
    LeaderElection leaderElection = new LeaderElection();
    leaderElection.connect();
    leaderElection.volunteerForLeadership();
    leaderElection.electLeader();
    leaderElection.run();
    leaderElection.close();

    System.out.println("I am done for the day.... Connect with you some other time... :)");
  }
}