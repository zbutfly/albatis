package net.butfly.albatis.kafka;

import java.util.List;

import kafka.consumer.ConsumerIterator;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import net.butfly.albacore.io.InputQueueImpl;
import net.butfly.albacore.lambda.Consumer;

class KafkaTopicInput extends InputQueueImpl<KafkaMessage, MessageAndMetadata<byte[], byte[]>> {
	private static final long serialVersionUID = 8996444280873898467L;
	private final Consumer<Integer> committing;
	private final ConsumerIterator<byte[], byte[]> iter;

	public KafkaTopicInput(String topic, ConsumerConnector connect, ConsumerIterator<byte[], byte[]> iter,
			Consumer<Integer> committing) {
		super("kafka-topic-queue-" + topic);
		this.committing = committing;
		this.iter = iter;
	}

	@Override
	protected KafkaMessage dequeueRaw() {
		MessageAndMetadata<byte[], byte[]> meta = iter.next();
		if (null == meta) return null;
		byte[] m = stats(Act.OUTPUT, meta).message();
		return new KafkaMessage(meta.topic(), meta.key(), m);
	}

	@Override
	public List<KafkaMessage> dequeue(long batchSize) {
		List<KafkaMessage> l = super.dequeue(batchSize);
		committing.accept(l.size());
		return l;
	}
}
