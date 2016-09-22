package net.butfly.albatis.kafka;

public class KafkaObjectWrapper<E> extends Wrapper {
	private static final long serialVersionUID = -748777815661568151L;
	private E value;

	public KafkaObjectWrapper(String topic, E value) {
		super(topic);
		this.value = value;
	}

	public KafkaObjectWrapper(String topic, Object key, E value) {
		super(topic, key);
		this.value = value;
	}

	public E getValue() {
		return value;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public void setKey(Object key) {
		this.key = key;
	}
}
