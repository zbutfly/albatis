package net.butfly.albatis.kafka;

import java.util.List;

import kafka.consumer.ConsumerIterator;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import net.butfly.albacore.io.InputQueueImpl;
import net.butfly.albacore.lambda.Converter;

class KafkaTopicInputQueue extends InputQueueImpl<Message, MessageAndMetadata<byte[], byte[]>> {
	private static final long serialVersionUID = 8996444280873898467L;
	private final Converter<List<Message>, List<Message>> committing;
	private final ConsumerIterator<byte[], byte[]> iter;

	public KafkaTopicInputQueue(String topic, ConsumerConnector connect, ConsumerIterator<byte[], byte[]> iter,
			Converter<List<Message>, List<Message>> committing) {
		super("kafka-topic-queue-" + topic, -1);
		this.committing = committing;
		this.iter = iter;
	}

	@Override
	public long size() {
		return Long.MAX_VALUE;
	}

	@Override
	protected Message dequeueRaw() {
		MessageAndMetadata<byte[], byte[]> meta = iter.next();
		if (null == meta) return null;
		byte[] m = stats(Act.OUTPUT, meta).message();
		return new Message(meta.topic(), meta.key(), m);
	}

	@Override
	public List<Message> dequeue(long batchSize) {
		return committing.apply(super.dequeue(batchSize));
	}
}
