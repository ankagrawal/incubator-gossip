package org.apache.gossip.consistency;

import java.util.HashMap;

public class Consistency {
	private ConsistencyLevel level;
	private HashMap<String, Object> parameters;
	
	public Consistency(ConsistencyLevel level, HashMap<String, Object> params) {
		this.level = level;
		this.parameters = params;
		if(this.parameters == null) {
			this.parameters = new HashMap<String, Object>();
		}
	}

	public ConsistencyLevel getLevel() {
		return level;
	}

	public void setLevel(ConsistencyLevel level) {
		this.level = level;
	}

	public HashMap<String, Object> getParameters() {
		return parameters;
	}

	public void setParameters(HashMap<String, Object> parameters) {
		this.parameters = parameters;
	}
	
	public void addParameter(String key, Object value) {
		this.parameters.put(key, value);
	}
}
