package net.butfly.albatis.impl.kafka.config;

import java.io.Serializable;

public class KafkaTopicConfig implements Serializable {
	private static final long serialVersionUID = -5726233255599028569L;
	private String topic;
	private int streamNum = 1;
	private String key;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getStreamNum() {
		return streamNum;
	}

	public void setStreamNum(int streamNum) {
		this.streamNum = streamNum;
	}
}
