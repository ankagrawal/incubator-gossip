package org.apache.gossip.manager;

import java.util.HashMap;
import java.util.List;

import org.apache.gossip.model.ReadWriteResponse;
import org.apache.gossip.model.Response;

public class MajorityResponseMerger implements ResponseMerger {
    public Response merge(List<? extends Response> responses) {
    	HashMap<Object, Integer> responseCount = new HashMap<Object, Integer>();
    	int majorityCount = 0;
    	Object majorityResult = null;
    	for(Response response : responses) {
    		ReadWriteResponse r = (ReadWriteResponse) response;
    		if(responseCount.containsKey(r.getValue())) {
    			responseCount.put(r.getValue(), responseCount.get(r.getValue()) + 1);
    		} else {
    			responseCount.put(r.getValue(), 1);
    		}
    		if(majorityCount < responseCount.get(r.getValue())) {
    			majorityCount = responseCount.get(r.getValue());
    			majorityResult = r.getKey();
    		}
    	}
    	return (Response)majorityResult;
    }
}
