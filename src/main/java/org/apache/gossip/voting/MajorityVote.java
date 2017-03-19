package org.apache.gossip.voting;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import org.apache.gossip.crdt.GrowOnlySet;

import com.fasterxml.jackson.annotation.JacksonInject;

public class MajorityVote<E> extends GrowOnlySet<Vote<E>> {

  private VotingContext votingContext;
  
  public MajorityVote(@JacksonInject final VotingContext votingContext){
    this.votingContext = votingContext;
  }

  public MajorityVote() {
    super();
  }

  public MajorityVote(Collection<Vote<E>> c) {
    super(c);
  }

  public MajorityVote(Set<Vote<E>> c) {
    super(c);
  }

  public MajorityVote<E> merge(MajorityVote<E> other) {
    Collection<Vote<E>> merged = new ArrayList<>();
    merged.addAll(this.value());
    merged.addAll(other.value());
    if (merged.size() == 0){
      throw new IllegalStateException("can not vote " +merged);
    }
    for (Vote<E> vote: merged){
      if (vote.getVoterId().equals(votingContext.parent.getMyself().getId())){
        return new MajorityVote<E>(merged);
      }
    }
    Vote<E> v = new Vote<>();
    v.setVoterId(votingContext.parent.getMyself().getId());
    v.setVote(merged.iterator().next().getVote());
    merged.add(v);
    return new MajorityVote<E>(merged);
  }

}
