package org.apache.gossip.voting;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.WRAPPER_OBJECT, property="type")
public class Vote<E> {

  private String voterId;
  private E vote;
  
  public Vote(){
    
  }

  public String getVoterId() {
    return voterId;
  }

  public void setVoterId(String voterId) {
    this.voterId = voterId;
  }

  public E getVote() {
    return vote;
  }

  public void setVote(E vote) {
    this.vote = vote;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((voterId == null) ? 0 : voterId.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    @SuppressWarnings("unchecked")
    Vote<E> other = (Vote<E>) obj;
    if (voterId == null) {
      if (other.voterId != null)
        return false;
    } else if (!voterId.equals(other.voterId))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "Vote [voterId=" + voterId + ", vote=" + vote + "]";
  }
 
}
