package net.butfly.albatis.kafka;

import java.util.concurrent.atomic.AtomicLong;

import kafka.consumer.ConsumerIterator;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import net.butfly.albacore.io.InputQueueImpl;
import net.butfly.albacore.utils.Systems;

class KafkaTopicInputQueue extends InputQueueImpl<Message, MessageAndMetadata<byte[], byte[]>> {
	private static final long serialVersionUID = 8996444280873898467L;
	private AtomicLong commitCount = new AtomicLong(0);
	private final ConsumerConnector connect;
	private final ConsumerIterator<byte[], byte[]> iter;

	public KafkaTopicInputQueue(String topic, ConsumerConnector connect, ConsumerIterator<byte[], byte[]> iter) {
		super("kafka-topic-queue-" + topic, -1);
		this.connect = connect;
		this.iter = iter;
		stats(byte[].class, b -> (long) b.length);
	}

	@Override
	public long size() {
		return Long.MAX_VALUE;
	}

	@Override
	@Deprecated
	protected boolean enqueueRaw(Void e) {
		return false;
	}

	@Override
	protected Message dequeueRaw() {
		MessageAndMetadata<byte[], byte[]> meta = iter.next();
		byte[] m = stats(Act.OUTPUT, meta.message(), () -> {
			long l = commitCount.incrementAndGet();
			if (l % 10000 == 0) {
				logger.trace(() -> "Kafka reading committed [" + (Systems.isDebug() ? "Dry" : "Wet") + "].");
				if (!Systems.isDebug()) connect.commitOffsets();
			}
			return l;
		});
		return new Message(meta.topic(), meta.key(), m);
	}
}
