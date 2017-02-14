package net.butfly.albatis.kafka;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.OpenableThread;
import net.butfly.albacore.io.OutputImpl;
import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;

/**
 * @author zx
 * @deprecated memory leak!
 */
@Deprecated
public final class KafkaOutput0 extends OutputImpl<KafkaMessage> {
	private final URISpec uri;
	private final KafkaOutputConfig config;
	private final Producer<byte[], byte[]> producer;
	private final LinkedBlockingQueue<List<KeyedMessage<byte[], byte[]>>> pool;
	private final OpenableThread[] senders = new OpenableThread[10];

	public KafkaOutput0(final String name, final String kafkaURI) throws ConfigException {
		super(name);
		pool = new LinkedBlockingQueue<>(10);
		uri = new URISpec(kafkaURI);
		config = new KafkaOutputConfig(uri);
		producer = new Producer<byte[], byte[]>(config.getConfig());
		Runnable t = () -> {
			try {
				producer.send(pool.take());
			} catch (InterruptedException e) {
				logger().error("Kafka sending interrupted");
			}
		};
		for (int i = 0; i < senders.length; i++) {
			senders[i] = new OpenableThread(t, name + "Sender#" + i);
			senders[i].open();
		}
		open();
	}

	@Override
	public void close() {
		super.close(() -> {
			for (int i = senders.length - 1; i >= 0; i--)
				senders[i].close();
			producer.close();
		});
	}

	@Override
	public boolean enqueue(KafkaMessage m) {
		if (null == m) return false;
		try {
			pool.put(Arrays.asList(m.toKeyedMessage()));
			return true;
		} catch (InterruptedException e) {
			logger().error("Kafka sending interrupted");
			return false;
		}
	}

	@Override
	public long enqueue(Stream<KafkaMessage> messages) {
		List<KeyedMessage<byte[], byte[]>> ms = io.list(messages.map(m -> m.toKeyedMessage()));
		try {
			pool.put(ms);
		} catch (InterruptedException e) {
			logger().error("Kafka sending interrupted");
			return 0;
		}
		return ms.size();
	}

	public long fails() {
		return 0;
	}

	public String getDefaultTopic() {
		String[] topics = config.getTopics();
		return topics == null || topics.length == 0 ? null : topics[0];
	}
}
