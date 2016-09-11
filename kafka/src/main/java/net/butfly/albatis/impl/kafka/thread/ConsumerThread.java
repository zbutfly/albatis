/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.butfly.albatis.impl.kafka.thread;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import net.butfly.albatis.impl.kafka.context.DataContext;
import net.butfly.albatis.impl.kafka.mapper.KafkaMessage;

import org.bson.BSONDecoder;
import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;

/**
 *
 * @author hzcominfo
 */
public class ConsumerThread extends Thread {

	private KafkaStream<byte[], byte[]> stream;
	private int msgListMax;
	private String keyFiled;
	private Map<String, String> topicKeyFieldMap;

	public void setTopicKeyFieldMap(Map<String, String> topicKeyFieldMap) {
		this.topicKeyFieldMap = topicKeyFieldMap;
	}

	public void setKeyFiled(String keyFiled) {
		this.keyFiled = keyFiled;
	}

	public void setMsgListMax(int msgListMax) {
		this.msgListMax = msgListMax;
	}

	public void setStream(KafkaStream<byte[], byte[]> stream) {
		this.stream = stream;
	}

	@Override
	public void run() {
		if (stream != null) {
			BSONDecoder decoder = new BasicBSONDecoder();
			for (MessageAndMetadata<byte[], byte[]> msgAndMetadata : stream) {
				String topic = msgAndMetadata.topic();
				if (keyFiled == null) {
					keyFiled = topicKeyFieldMap.get(topic);
				}
				byte[] message = msgAndMetadata.message();
				BSONObject value = decoder.readObject(message);
				KafkaMessage kafkaMsg = new KafkaMessage();
				kafkaMsg.setTopic(topic);
				kafkaMsg.setValues((Map<String, Object>) value.get("value"));
				if (kafkaMsg.getValues().containsKey(keyFiled)) {
					kafkaMsg.setKeyValue(kafkaMsg.getValues().get(keyFiled));
				}
				while (DataContext.msgList.size() > msgListMax) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException ex) {
						Logger.getLogger(ConsumerThread.class.getName()).log(Level.SEVERE, null, ex);
					}
				}

				DataContext.msgList.add(kafkaMsg);

			}
		}
	}

}
