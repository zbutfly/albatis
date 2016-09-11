package net.butfly.albatis.dao;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.butfly.albacore.entity.AbstractEntity;
import net.butfly.albacore.utils.Generics;
import net.butfly.albatis.impl.kafka.api.KafkaConsumer;
import net.butfly.albatis.impl.kafka.config.KafkaConsumerConfig;
import net.butfly.albatis.impl.kafka.mapper.KafkaMessage;
import net.butfly.albatis.impl.kafka.mapper.KafkaTopicConfig;

public class KafkaDaoBase implements KafkaDao {
	private static final long serialVersionUID = 399002866318381750L;
	private KafkaConsumer consumers = null;
	private KafkaConsumerConfig consumer = null;
	// private KafkaProducerConfig producerConfig;
	private KafkaTopicConfig[] topics = null;
	private boolean bufferMix;
	private int msgBuffers;
	private int bufferMax;

	@Override
	public void setConsumer(KafkaConsumerConfig consumer) {}

	public void setTopics(KafkaTopicConfig[] topics) {
		this.topics = topics;
	}

	public void setBufferMix(boolean bufferMix) {
		this.bufferMix = bufferMix;
	}

	public void setMsgBuffers(int msgBuffers) {
		this.msgBuffers = msgBuffers;
	}

	public void setBufferMax(int bufferMax) {
		this.bufferMax = bufferMax;
	}

	@Override
	protected void finalize() {
		if (null != consumers) consumers.uninit();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> E[] select(Class<E> entityClass, K... topic) {
		Class<K> kc = AbstractEntity.getKeyClass((Class<? extends AbstractEntity<?>>) entityClass);
		if (!CharSequence.class.isAssignableFrom(kc)) throw new RuntimeException(
				"Kafka only support String key entity. You can use KafkaEntity");
		List<E> results = new ArrayList<>();
		if (topic != null && topic.length > 0) {
			Set<K> topics = new HashSet<>(Arrays.asList(topic));
			for (String k : (Set<String>) topics) {
				if ("".equals(k)) throw new RuntimeException(
						"Since key is defines, you could not use empty (which means fetch any topic.)");
				List<KafkaMessage> ml = consumers.getMessagePackage(k);
				consumers.commit();
				for (KafkaMessage m : ml)
					results.add(deserialize(m.getKeyValue(), m.getValues()));

			}
		} else if (bufferMix) {
			List<KafkaMessage> ml = consumers.getMessagePackage("");
			consumers.commit();
			for (KafkaMessage m : ml)
				results.add(deserialize(m.getKeyValue(), m.getValues()));
		} else throw new RuntimeException("No topics, and not mix, we could not fetch any message from kafka.");
		return Generics.toArray(results, entityClass);
	}

	private <K extends Serializable, E extends AbstractEntity<K>> E deserialize(Object keyValue, Map<String, Object> values) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void init() {
		if (consumers != null && topics != null) {
			consumers = new KafkaConsumer();
			consumers.init(consumer, topics, bufferMix, msgBuffers, bufferMax);
		}
	}
}
