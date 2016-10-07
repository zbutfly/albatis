package net.butfly.albatis.kafka;

import java.util.concurrent.atomic.AtomicLong;

import kafka.consumer.ConsumerIterator;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import net.butfly.albacore.io.AbstractQueue;
import net.butfly.albacore.utils.Systems;

class InputTopicQueue extends AbstractQueue<Message, Message, Message> {
	private static final long serialVersionUID = 8996444280873898467L;
	private AtomicLong count = new AtomicLong(0);
	private final ConsumerConnector connect;
	private final ConsumerIterator<byte[], byte[]> iter;

	public InputTopicQueue(String topic, ConsumerConnector connect, ConsumerIterator<byte[], byte[]> iter) {
		super("kafka-topic-queue-" + topic, -1, v -> null, m -> m);
		this.connect = connect;
		this.iter = iter;
		stats(byte[].class, b -> (long) b.length);
	}

	@Override
	public long size() {
		return -1;
	}

	@Override
	protected boolean enqueueRaw(Message e) {
		return false;
	}

	@Override
	protected Message dequeueRaw() {
		MessageAndMetadata<byte[], byte[]> meta = iter.next();
		byte[] m = stats(Act.OUTPUT, meta.message(), () -> {
			long l = count.incrementAndGet();
			if (l % 10000 == 0) {
				logger.trace(() -> "Kafka reading committed [" + (Systems.isDebug() ? "Dry" : "Wet") + "].");
				if (!Systems.isDebug()) connect.commitOffsets();
			}
			return l;
		});
		Message km = new Message(meta.topic(), meta.key(), m);
		return new Message(Message.SERDER.ser(km));
	}
}
