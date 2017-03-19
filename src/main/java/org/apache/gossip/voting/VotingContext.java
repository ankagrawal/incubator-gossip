package org.apache.gossip.voting;

import org.apache.gossip.manager.GossipManager;

public class VotingContext {

  protected GossipManager parent;
  
  public VotingContext(GossipManager manager){
    parent = manager;
  }
  
}
