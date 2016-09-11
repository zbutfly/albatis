package net.butfly.albatis.impl.kafka.mapper;

import java.io.Serializable;

public class KafkaTopicConfig implements Serializable {
	private static final long serialVersionUID = -5726233255599028569L;
	private String topic;
	private Integer streamNum;
	private String key;

	public String getKey() {
		if (key == null) { return ""; }
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getTopic() {
		if (topic == null) { return null; }
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public Integer getStreamNum() {
		if (streamNum == null) { return 0; }
		return streamNum;
	}

	public void setStreamNum(Integer streamNum) {
		this.streamNum = streamNum;
	}

}
