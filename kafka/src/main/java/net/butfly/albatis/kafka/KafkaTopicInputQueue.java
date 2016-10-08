package net.butfly.albatis.kafka;

import java.util.concurrent.atomic.AtomicLong;

import kafka.consumer.ConsumerIterator;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import net.butfly.albacore.io.InputQueueImpl;
import net.butfly.albacore.io.stats.Statistical;
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
		this.statsRegister(e -> null == e ? Statistical.SIZE_NULL : e.message().length, Act.OUTPUT);
	}

	@Override
	public long size() {
		return Long.MAX_VALUE;
	}

	@Override
	protected Message dequeueRaw() {
		MessageAndMetadata<byte[], byte[]> meta = iter.next();
		if (null == meta) return null;
		byte[] m = statsRecord(Act.OUTPUT, meta, () -> {
			long l = commitCount.incrementAndGet();
			if (l % 10000 == 0) {
				logger.trace(() -> "Kafka reading committed [" + (Systems.isDebug() ? "Dry" : "Wet") + "].");
				if (!Systems.isDebug()) connect.commitOffsets();
			}
			return l;
		}).message();
		return new Message(meta.topic(), meta.key(), m);
	}
}
