package net.butfly.albatis.kafka;

import java.util.List;

import kafka.consumer.ConsumerIterator;
import kafka.message.MessageAndMetadata;
import net.butfly.albacore.io.InputQueue;
import net.butfly.albacore.io.InputQueueImpl;
import net.butfly.albacore.utils.Systems;

class KafkaTopicInput extends InputQueueImpl<KafkaMessage> implements InputQueue<KafkaMessage> {
	private static final long serialVersionUID = 8996444280873898467L;
	private final Runnable committing;
	private final ConsumerIterator<byte[], byte[]> iter;
	private final String topic;

	public KafkaTopicInput(String topic, ConsumerIterator<byte[], byte[]> iter, Runnable committing) {
		super("kafka-topic-queue-" + topic);
		this.topic = topic;
		this.iter = iter;
		this.committing = committing;
	}

	@Override
	protected KafkaMessage dequeueRaw() {
		MessageAndMetadata<byte[], byte[]> meta = iter.next();
		return null == meta ? null : new KafkaMessage(topic, meta.key(), meta.message());
	}

	@Override
	public List<KafkaMessage> dequeue(long batchSize) {
		try {
			return super.dequeue(batchSize);
		} finally {
			if (Systems.isDebug()) committing.run();
		}
	}

	@Override
	public KafkaMessage dequeue() {
		try {
			return super.dequeue();
		} finally {
			if (Systems.isDebug()) committing.run();
		}
	}
}
