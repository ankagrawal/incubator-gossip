package org.apache.gossip.consistency;

import java.util.UUID;
import java.util.concurrent.Callable;

import org.apache.gossip.LocalMember;
import org.apache.gossip.Member;
import org.apache.gossip.manager.GossipCore;
import org.apache.gossip.model.Request;
import org.apache.gossip.model.Response;
import org.apache.gossip.udp.UdpReadRequest;

public class RemoteRequestCallable implements Callable<Response> {
	Request request;
	LocalMember to;
	LocalMember from;
	GossipCore gossipCore;
    
	public RemoteRequestCallable(Request request, Member from, LocalMember to, GossipCore gossipCore) {
		this.request = request;
		this.to = to;
		this.gossipCore = gossipCore;
	}
	
	public Response call() {
		UdpReadRequest udpRequest = (UdpReadRequest)request;
		udpRequest.setUriFrom(from.getId());
		udpRequest.setUuid(UUID.randomUUID().toString());
		Response r = gossipCore.send(udpRequest, to.getUri());
		return r;
	}
}
