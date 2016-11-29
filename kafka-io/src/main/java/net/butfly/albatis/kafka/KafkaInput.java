package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.producer.KafkaProducer;

import kafka.consumer.Consumer;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.MapInput;
import net.butfly.albacore.io.queue.Q;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.kafka.config.KafkaInputConfig;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;

public class KafkaInput extends MapInput<String, KafkaMessage> {
	private static final long serialVersionUID = 7617065839861658802L;
	private static final Logger logger = Logger.getLogger(KafkaInput.class);
	private final ConsumerConnector connect;
	private final Map<String, Integer> topics;
	private final Map<String, List<KafkaStream<byte[], byte[]>>> streams;
	private final Map<KafkaStream<byte[], byte[]>, ReentrantLock> locks;

	public KafkaInput(String name, final String config, String... topic) throws ConfigException, IOException {
		super(name);
		KafkaInputConfig kic = new KafkaInputConfig(config);
		topics = new HashMap<>();
		if (kic.isParallelismEnable()) try (KafkaProducer<byte[], byte[]> kc = new KafkaProducer<>(new KafkaOutputConfig(config)
				.props());) {
			for (String t : topic)
				f(t, kc.partitionsFor(t).size());
		}
		else for (String t : topic)
			f(t, 1);

		logger.debug("Kafka [" + config.toString() + "] connecting.");
		connect = Consumer.createJavaConsumerConnector(kic.getConfig());
		logger.debug("Kafka [" + config.toString() + "] Connected.");
		streams = connect.createMessageStreams(topics);
		locks = new HashMap<>();
		for (List<KafkaStream<byte[], byte[]>> l : streams.values())
			for (KafkaStream<byte[], byte[]> s : l)
				locks.put(s, new ReentrantLock());
		logger.debug("KafkaInput ready in [" + streams.size() + "] topics: " + topics.toString());
	}

	private void f(String t, int partitions) {
		logger.info("Topic [" + t + "] partitions detected: " + partitions);
		topics.put(t, partitions);
	}

	@Override
	public KafkaMessage dequeue0(String topic) {
		for (KafkaStream<byte[], byte[]> s : streams.get(topic)) {
			KafkaMessage e = fetch(s, v -> connect.commitOffsets(false));
			if (null != e) return e;
		}
		return null;
	}

	@Override
	public List<KafkaMessage> dequeue(long batchSize, String... topic) {
		List<KafkaMessage> batch = new ArrayList<>();
		long prev;
		try {
			do {
				prev = batch.size();
				for (String t : topic)
					for (KafkaStream<byte[], byte[]> s : streams.get(t))
						fetch(s, e -> batch.add(e));
				if (batch.size() >= batchSize) return batch;
				if (batch.size() == 0) Concurrents.waitSleep(EMPTY_WAIT_MS);
			} while (batch.size() < batchSize && (prev != batch.size() || batch.size() == 0));
			return batch;
		} finally {
			connect.commitOffsets(true);
		}
	}

	@Override
	public List<KafkaMessage> dequeue(long batchSize) {
		return dequeue(batchSize, keys().toArray(new String[0]));
	}

	@Override
	public boolean empty(String key) {
		return false;
	}

	@Override
	public long size() {
		return Long.MAX_VALUE;
	}

	@Override
	public long size(String key) {
		return Long.MAX_VALUE;
	}

	@Override
	public Set<String> keys() {
		return streams.keySet();
	}

	@Override
	public Q<Void, KafkaMessage> q(String key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() {
		logger.debug("KafkaInput [" + name() + "] closing...");
		connect.commitOffsets(true);
		connect.shutdown();
		logger.info("KafkaInput [" + name() + "] closed.");
	}

	public long poolStatus() {
		return -1;
	}

	private KafkaMessage fetch(KafkaStream<byte[], byte[]> stream, net.butfly.albacore.lambda.Consumer<KafkaMessage> result) {
		ReentrantLock lock = locks.get(stream);
		try {
			if (!lock.tryLock(10, TimeUnit.MILLISECONDS)) return null;
		} catch (InterruptedException e) {
			return null;
		}
		MessageAndMetadata<byte[], byte[]> e = null;
		try {
			e = stream.iterator().next();
		} catch (Exception ex) {} finally {
			lock.unlock();
		}
		if (null != e) {
			KafkaMessage km = new KafkaMessage(e);
			result.accept(km);
			return km;
		} else return null;
	}
}
