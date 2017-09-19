package org.apache.gossip.model;

public class WriteRequestMessage extends Request {
    private String key;
    private Object value;
	private Long timestamp;
	private Long expireAt;

	public WriteRequestMessage(String key, Object value) {
		this.key = key;
		this.value = value;
	}
	
	public WriteRequestMessage() { }
	
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public Object getValue() {
		return value;
	}
	public void setValue(Object value) {
		this.value = value;
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
}
