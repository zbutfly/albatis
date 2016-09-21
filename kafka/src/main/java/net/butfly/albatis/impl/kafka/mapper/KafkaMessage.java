package net.butfly.albatis.impl.kafka.mapper;

import java.util.Map;

public class KafkaMessage {
	private String topic;
	private Map<String, Object> value;
	private Object key;

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public Map<String, Object> getValue() {
		return value;
	}

	public void setValue(Map<String, Object> value) {
		this.value = value;
	}

	public Object get(String prop) {
		return value.get(prop);
	}

	public void put(String prop, Object v) {
		value.put(prop, v);
	}

	public Object getKey() {
		return key;
	}

	public void setKey(Object key) {
		this.key = key;
	}
}
