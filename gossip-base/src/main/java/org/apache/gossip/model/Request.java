package org.apache.gossip.model;

public class Request extends Message {
    private RequestAction action;

	public RequestAction getAction() {
		return action;
	}

	public void setAction(RequestAction action) {
		this.action = action;
	}
}
