package org.apache.gossip.manager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.gossip.LocalMember;
import org.apache.gossip.Member;
import org.apache.gossip.consistency.Consistency;
import org.apache.gossip.consistency.ConsistencyLevel;
import org.apache.gossip.consistency.RemoteRequestCallable;
import org.apache.gossip.model.Request;
import org.apache.gossip.model.Response;

public class Coordinator {
    ExecutorService executor;
    
    public Coordinator() {
    	executor = Executors.newCachedThreadPool();
    }
    
    private List<Response> handleAll(List<Future<Response>> futures,
    		ExecutorCompletionService<Response> ecs) {
    	int futureSize = futures.size();
    	List<Response> responses = new ArrayList<Response>();
    	while(futureSize > 0) {
    		try {
    		    responses.add(ecs.take().get());
    		    futureSize--;
    		} catch(Exception ex) {
    			System.out.println(ex.toString());
    		}
    	}
    	return responses;
    }
    
    public List<Response> coordinateRequest(List<? extends Member> members, Request request,
    		Consistency con, LocalMember me, GossipCore gossipCore) {
    	ExecutorCompletionService<Response> ecs = new ExecutorCompletionService<Response>(executor);
    	List<Future<Response>> futures = new ArrayList<Future<Response>>();
    	for(Member member : members) {
    		RemoteRequestCallable remoteRequest = new RemoteRequestCallable(request, member, me, gossipCore);
    		futures.add(ecs.submit(remoteRequest));
    	}
    	if(con.getLevel() == ConsistencyLevel.ALL)
    		return handleAll(futures, ecs);
    	else if(con.getLevel() == ConsistencyLevel.N)
    		return handleN(futures, ecs);
    	else if(con.getLevel() == ConsistencyLevel.ANY)
    		return handleAny(futures, ecs);
    	return null;
    }
}