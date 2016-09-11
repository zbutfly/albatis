/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.butfly.albatis.impl.kafka.mapper;

import java.util.Map;

/**
 *
 * @author hzcominfo
 */
public class KafkaMessage {
	private String topic;
	private Map<String, Object> values;
	// value of key field
	private Object keyValue;

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public Map<String, Object> getValues() {
		return values;
	}

	public void setValues(Map<String, Object> values) {
		this.values = values;
	}

	public Object getKeyValue() {
		if (keyValue == null) { return ""; }
		return keyValue;
	}

	public void setKeyValue(Object keyValue) {
		this.keyValue = keyValue;
	}
}
