package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.MapInput;
import net.butfly.albacore.io.queue.Q;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.kafka.config.KafkaInputConfig;

public class KafkaInput extends MapInput<String, KafkaMessage> {
	private static final long serialVersionUID = 7617065839861658802L;
	private static final Logger logger = Logger.getLogger(KafkaInput.class);
	private final ConsumerConnector connect;
	private final Map<String, List<Integer>> order;
	private final Map<String, List<KafkaStream<byte[], byte[]>>> topicStreams;
	// private KafkaConsumer<byte[], byte[]> consumer;

	public KafkaInput(String name, final String config, final Map<String, Integer> topics) throws ConfigException, IOException {
		this(name, new KafkaInputConfig(config), topics);
	}

	public KafkaInput(String name, final KafkaInputConfig config, final Map<String, Integer> topics) throws ConfigException {
		super(name);
		order = new HashMap<>();
		for (String topic : topics.keySet()) {
			Integer[] ints = new Integer[topics.get(topic)];
			for (int i = 0; i < ints.length; i++)
				ints[i] = i;
			List<Integer> l = Arrays.asList(ints);
			order.put(topic, l);
		}

		logger.debug("Kafka [" + config.toString() + "] connecting.");
		connect = Consumer.createJavaConsumerConnector(config.getConfig());
		logger.debug("Kafka [" + config.toString() + "] Connected.");
		topicStreams = connect.createMessageStreams(topics);

//		long fucking = Long.parseLong(System.getProperty("albatis.kafka.fucking.waiting", "15000"));
//		logger.error("FFFFFFFucking lazy initialization of Kafka, we are sleeping [" + fucking + "ms].");
//		Concurrents.waitSleep(fucking);
//		logger.error("We had waked up, are you ok, Kafka?");
		logger.debug("KafkaInput ready in [" + topicStreams.size() + "] topics: " + topics.toString());
	}

	@Override
	public KafkaMessage dequeue0(String topic) {
		List<KafkaStream<byte[], byte[]>> l = topicStreams.get(topic);
		for (Integer i : order.compute(topic, (k, v) -> Collections.disorderize(v))) {
			ConsumerIterator<byte[], byte[]> it = l.get(i).iterator();
			if (it.hasNext()) return new KafkaMessage(it.next());
		}
		return null;
	}

	@Override
	public List<KafkaMessage> dequeue(long batchSize, String... topic) {
		List<KafkaMessage> batch = new ArrayList<>();
		long prev;
		do {
			prev = batch.size();
			for (String t : topic) {
				for (KafkaStream<byte[], byte[]> s : topicStreams.get(t)) {
					ConsumerIterator<byte[], byte[]> it = s.iterator();
					if (it.hasNext()) batch.add(new KafkaMessage(it.next()));
				}
			}
			if (batch.size() >= batchSize) return batch;
			if (batch.size() == 0) Concurrents.waitSleep(EMPTY_WAIT_MS);
		} while (batch.size() < batchSize && (prev != batch.size() || batch.size() == 0));
		return batch;
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
		return topicStreams.keySet();
	}

	@Override
	public Q<Void, KafkaMessage> q(String key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() {
		connect.shutdown();
	}
}
