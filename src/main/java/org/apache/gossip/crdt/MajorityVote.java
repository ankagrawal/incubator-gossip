package org.apache.gossip.crdt;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.gossip.model.Pair;
import org.apache.gossip.voting.Vote;
import org.apache.gossip.voting.VotingContext;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;


public class MajorityVote<E> extends OrSet<Vote<E>> {

  private transient VotingContext votingContext;
  
  public MajorityVote(@JacksonInject final VotingContext votingContext, 
          @JsonProperty("el") List<Pair<Vote<E>,List<UUID>>> elements, 
          @JsonProperty("to") List<Pair<Vote<E>,List<UUID>>> tombstones){    
    this.votingContext = votingContext;
    for (Pair<Vote<E>, List<UUID>> l:elements){
      this.elements.put(l.getLeft(), new HashSet<UUID>(l.getRight()));
    }
    for (Pair<Vote<E>, List<UUID>> l:tombstones){
      this.elements.put(l.getLeft(), new HashSet<UUID>(l.getRight()));
    }
    val = computeValue();
    maybeVote();
  }

  /**
   * If this node has not voted we need to vote. General strategy is agree with anyone and hopefully we converge  
   */
  private void maybeVote() {
    if (val.size() == 0){
      return;
    }
    for (Vote<E> vote : val){
      if(votingContext.getParent().getMyself().getId().equals(vote.getVoterId())){
        return;
      }
    }
    Vote<E> v = new Vote<>();
    v.setVoterId(votingContext.getParent().getMyself().getId());
    v.setVote(val.iterator().next().getVote());
    Set<UUID> s = new HashSet<>();
    s.add(UUID.randomUUID());
    elements.put(v, s);
    val = computeValue();
  }

  public MajorityVote() {
    super();
  }
  
  public MajorityVote(OrSet.Builder<Vote<E>> builder) {
    super(builder);
  }
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @JsonProperty("el") List<Pair<Vote<E>,List<UUID>>> getEl(){
    List<Pair<Vote<E>,List<UUID>>> p = new ArrayList<>();
    for (Entry<Vote<E>, Set<UUID>> l : super.getElements().entrySet()){
      p.add(new Pair(l.getKey(), new ArrayList<UUID>(l.getValue()))); 
    }
    return p;
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @JsonProperty("to") List<Pair<Vote<E>,List<UUID>>> geTo(){
    List<Pair<Vote<E>,List<UUID>>> p = new ArrayList<>();
    for (Entry<Vote<E>, Set<UUID>> l : super.getTombstones().entrySet()){
      p.add(new Pair(l.getKey(), new ArrayList<UUID>(l.getValue()))); 
    }
    return p;
  }

}
