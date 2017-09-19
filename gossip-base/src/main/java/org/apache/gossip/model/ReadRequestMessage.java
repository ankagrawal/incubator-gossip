package org.apache.gossip.model;

public class ReadRequestMessage extends Request {
	private String key;
	private Long timestamp;
	private Long expireAt;

	public ReadRequestMessage(String key) {
		this.key = key;
	}
	
	public ReadRequestMessage() { }

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public Long getExpireAt() {
		return expireAt;
	}

	public void setExpireAt(Long expireAt) {
		this.expireAt = expireAt;
	}

	@Override
	public String toString() {
	    return "ReadRequestMessage [, key=" + key
	            + ", timestamp=" + timestamp + ", expireAt=" + expireAt + "]";
	}
}
