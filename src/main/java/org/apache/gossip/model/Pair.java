package org.apache.gossip.model;

public class Pair<Left,Right> {

  private Left left;
  private Right right;
  
  public Pair(){
    
  }

  public Pair(Left left, Right right){
    this.left = left;
    this.right = right;
  }
  public Left getLeft() {
    return left;
  }

  public void setLeft(Left left) {
    this.left = left;
  }

  public Right getRight() {
    return right;
  }

  public void setRight(Right right) {
    this.right = right;
  }
  
}
