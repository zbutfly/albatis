package net.butfly.albatis.kafka;

import java.io.Serializable;

public abstract class KafkaWrapper implements Serializable {
	private static final long serialVersionUID = -6501558812416016470L;
	protected String topic;
	protected Object key;

	protected KafkaWrapper() {
		super();
	}

	protected KafkaWrapper(String topic) {
		super();
		this.topic = topic;
	}

	protected KafkaWrapper(String topic, Object key) {
		this(topic);
		this.key = key;
	}

	public String getTopic() {
		return topic;
	}

	public Object getKey() {
		return key;
	}
}
