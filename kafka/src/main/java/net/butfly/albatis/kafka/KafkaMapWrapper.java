package net.butfly.albatis.kafka;

import java.util.Map;

public class KafkaMapWrapper extends Wrapper {
	private static final long serialVersionUID = 2789277195011553962L;
	private Map<String, Object> meta;
	private Map<String, Object> value;

	public KafkaMapWrapper(String topic, Map<String, Object> value) {
		super(topic);
		this.value = value;
	}

	public KafkaMapWrapper(String topic, Object key, Map<String, Object> value) {
		super(topic, key);
		this.value = value;
	}

	public KafkaMapWrapper(String topic, Map<String, Object> meta, Map<String, Object> value) {
		this(topic, value);
		this.meta = meta;
	}

	public KafkaMapWrapper(String topic, Object key, Map<String, Object> meta, Map<String, Object> value) {
		this(topic, key, value);
		this.meta = meta;
	}

	public Map<String, Object> getMeta() {
		return meta;
	}

	@SuppressWarnings("unchecked")
	public <T> T getMeta(String prop, T... defaults) {
		T t = (T) meta.get(prop);
		for (int i = 0; t == null && i < defaults.length; i++)
			t = defaults[i];
		return t;
	}

	public Map<String, Object> getValue() {
		return value;
	}

	@SuppressWarnings("unchecked")
	public <T> T getValue(String prop) {
		return (T) value.get(prop);
	}
}
