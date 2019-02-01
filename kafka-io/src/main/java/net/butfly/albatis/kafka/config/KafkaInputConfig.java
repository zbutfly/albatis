package net.butfly.albatis.kafka.config;

import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Pair;

/**
 * Valid prop names:
 * <ul>
 * <li>zookeeper.connect</li>
 * <li>group.id</li>
 * <li>auto.commit.enable</li>
 * <li>auto.commit.interval.ms</li>
 * <li>auto.offset.reset</li>
 * <li>partition.assignment.strategy</li>
 * <li>zookeeper.connection.timeout.ms</li>
 * <li>zookeeper.session.timeout.ms</li>
 * <li>zookeeper.sync.time.ms</li>
 * <li>fetch.message.max.bytes</li>
 * <li>fetch.wait.max.ms</li>
 * <li>socket.timeout.ms</li>
 * <li>socket.receive.buffer.bytes</li>
 * <li>consumer.timeout.ms</li>
 * <li>rebalance.backoff.ms</li>
 * <li>rebalance.max.retries</li>
 * </ul>
 * 
 * @author zx
 *
 */
public class KafkaInputConfig extends Kafka2InputConfig {
	private static final long serialVersionUID = 3940044434776160500L;

	public KafkaInputConfig(String consumerId, URISpec uri) {
		super(consumerId, uri);
		if (null == fetchWaitTimeoutMs) fetchWaitTimeoutMs = 0L;
	}

	@Deprecated
	public KafkaInputConfig(String consumerId, Properties props) {
		super(consumerId, props);
		if (null == fetchWaitTimeoutMs) fetchWaitTimeoutMs = 0L;
	}

	public ConsumerConfig getConfig() throws ConfigException {
		// if (zookeeperConnect == null || groupId == null) throw new ConfigException(
		// "Kafka configuration has no zookeeper and group definition.");
		ConsumerConfig conf = new ConsumerConfig(props());
		return conf;
	}

	public Map<String, Integer> getTopicPartitions(String... topics) {
		return KafkaZkParser.getTopicPartitions(zookeeperConnect, groupId, topics);
	}

	@Override
	protected String bootstrapFromZk(String zk) {
		return KafkaZkParser.bootstrapFromZk(zk);
	}

	@Override
	protected Pair<String, String> bootstrapFromZk(URISpec uri) {
		return KafkaZkParser.bootstrapFromZk(uri);
	}
}
