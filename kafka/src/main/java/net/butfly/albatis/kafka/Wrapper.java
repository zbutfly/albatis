package net.butfly.albatis.kafka;

import java.io.Serializable;

public abstract class Wrapper implements Serializable {
	private static final long serialVersionUID = -6501558812416016470L;
	protected String topic;
	protected Object key;

	protected Wrapper() {
		super();
	}

	protected Wrapper(String topic) {
		super();
		this.topic = topic;
	}

	protected Wrapper(String topic, Object key) {
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
