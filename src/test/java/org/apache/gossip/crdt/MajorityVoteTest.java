package org.apache.gossip.crdt;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.gossip.GossipSettings;
import org.apache.gossip.manager.GossipManager;
import org.apache.gossip.manager.GossipManagerBuilder;
import org.apache.gossip.voting.Vote;
import org.junit.Assert;
import org.junit.Test;

public class MajorityVoteTest {
  
  private MajorityVote<String> newVote(){
    Vote<String> vote = new Vote<>();
    vote.setVote("a");
    vote.setVoterId("1");
    MajorityVote<String> s = new MajorityVote<String>
      (new OrSet.Builder<Vote<String>>().add(vote));
    return s;
  }
  
  private GossipManager server2() throws URISyntaxException {
    return GossipManagerBuilder.newBuilder()
            .cluster("a")
            .uri(new URI("udp://" + "127.0.0.1" + ":" + (29000 + 2)))
            .id("2")
            .gossipSettings(new GossipSettings())
            .build();
  }
  
  private GossipManager server1() throws URISyntaxException {
    return GossipManagerBuilder.newBuilder()
    .cluster("a")
    .uri(new URI("udp://" + "127.0.0.1" + ":" + (29000 + 1)))
    .id("1")
    .gossipSettings(new GossipSettings())
    .build();
  }
  
  @Test
  public void serdeTest() throws URISyntaxException, IOException{
    GossipManager server1 = server1();
    GossipManager server2 = server2();
    
    String deser = server1.getObjectMapper().writeValueAsString(newVote());
    @SuppressWarnings("unchecked")
    MajorityVote<String> vote1 = (MajorityVote<String>) 
    server1.getObjectMapper().readValue(deser, MajorityVote.class);
    Assert.assertEquals(vote1.value().iterator().next().getVote(), 
            newVote().iterator().next().getVote());
    Assert.assertEquals(1, vote1.value().size());
    
    @SuppressWarnings("unchecked")
    MajorityVote<String> result = server2.getObjectMapper().readValue(deser, MajorityVote.class);
    //System.out.println(result);
    Assert.assertEquals(2, result.value().size());
    for (Vote<String> l :result.value()){
      Assert.assertEquals("a", l.getVote());
    }
  }
}
