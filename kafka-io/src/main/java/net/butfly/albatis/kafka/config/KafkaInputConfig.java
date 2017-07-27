package net.butfly.albatis.kafka.config;

import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.utils.URISpec;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.logger.Logger;

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
public class KafkaInputConfig extends KafkaConfigBase {
	private static final long serialVersionUID = -3028341800709486625L;
	private static final Logger logger = Logger.getLogger(KafkaInputConfig.class);
	private static final String DEFAULT_AUTO_COMMIT_MS = "60000";

	protected long zookeeperSyncTimeMs;
	protected final String consumerId;
	protected final String groupId;
	protected boolean autoCommitEnable;
	protected long autoCommitIntervalMs;
	protected String autoOffsetReset;
	protected long sessionTimeoutMs;
	protected String partitionAssignmentStrategy;
	protected long fetchMessageMaxBytes;
	protected long fetchWaitTimeoutMs;
	protected int rebalanceRetries;
	protected long zookeeperSessionTimeoutMs;

	// not for kafka, for albatis
	private int partitionParallelism;

	public KafkaInputConfig(String consumerId, URISpec uri) {
		super(uri);
		this.consumerId = consumerId;
		groupId = calcGroupId(uri.getUsername());
		Map<String, String> props = uri.getParameters();

		zookeeperSyncTimeMs = Long.parseLong(props.getOrDefault("zksynctime", "15000").trim());
		autoCommitIntervalMs = Long.parseLong(props.getOrDefault("autocommit", DEFAULT_AUTO_COMMIT_MS).trim());
		autoCommitEnable = autoCommitIntervalMs > 0;
		autoOffsetReset = props.getOrDefault("autoreset", "smallest");
		sessionTimeoutMs = Long.parseLong(props.getOrDefault("sessiontimeout", "30000").trim());
		fetchWaitTimeoutMs = Long.parseLong(props.getOrDefault("fetchtimeout", "500").trim());
		partitionAssignmentStrategy = props.getOrDefault("strategy", "range");
		fetchMessageMaxBytes = Long.parseLong(props.getOrDefault("fetchmax", "10485760").trim());
		rebalanceRetries = Integer.parseInt(props.getOrDefault("rebalanceretries", "2"));
		zookeeperSessionTimeoutMs = Long.parseLong(props.getOrDefault("zksessiontimeout", "30000"));

		partitionParallelism = Integer.parseInt(props.getOrDefault("parallelism", "0").trim());
	}

	/**
	 * @deprecated use {@link URISpec} to construct kafka configuration.
	 */
	@Deprecated
	public KafkaInputConfig(String consumerId, Properties props) {
		super(props);
		this.consumerId = consumerId;
		groupId = calcGroupId(props.getProperty("albatis.kafka.group.id"));

		zookeeperSyncTimeMs = Long.parseLong(props.getProperty(PROP_PREFIX + "zookeeper.sync.time.ms", "15000").trim());
		autoCommitIntervalMs = Long.parseLong(props.getProperty(PROP_PREFIX + "auto.commit.interval.ms", DEFAULT_AUTO_COMMIT_MS).trim());
		autoCommitEnable = Boolean.parseBoolean(props.getProperty(PROP_PREFIX + "auto.commit.enable", Boolean.toString(
				autoCommitIntervalMs > 0)).trim());
		autoOffsetReset = props.getProperty(PROP_PREFIX + "auto.offset.reset", "smallest");
		sessionTimeoutMs = Long.parseLong(props.getProperty(PROP_PREFIX + "session.timeout.ms", "30000").trim());
		fetchWaitTimeoutMs = Long.parseLong(props.getProperty(PROP_PREFIX + "fetch.wait.timeout.ms", "500").trim());
		partitionAssignmentStrategy = props.getProperty(PROP_PREFIX + "partition.assignment.strategy", "range");
		fetchMessageMaxBytes = Long.parseLong(props.getProperty(PROP_PREFIX + "fetch.message.max.bytes", "10485760").trim());
		rebalanceRetries = Integer.parseInt(props.getProperty(PROP_PREFIX + "rebalance.retries", "2"));
		zookeeperSessionTimeoutMs = Long.parseLong(props.getProperty(PROP_PREFIX + "zookeeper.session.timeout.ms", "30000"));

		partitionParallelism = Integer.parseInt(props.getProperty(PROP_PREFIX + "partition.parallelism", "0").trim());
	}

	private static String calcGroupId(String configGroupId) {
		if (configGroupId == null || "".equals(configGroupId)) configGroupId = Systems.getMainClass().getSimpleName();
		configGroupId = Systems.suffixDebug(configGroupId, logger);
		return configGroupId;
	}

	public int getPartitionParallelism() {
		return partitionParallelism;
	}

	public ConsumerConfig getConfig() throws ConfigException {
		if (zookeeperConnect == null || groupId == null) throw new ConfigException(
				"Kafka configuration has no zookeeper and group definition.");
		ConsumerConfig conf = new ConsumerConfig(props());
		return conf;
	}

	@Override
	public Properties props() {
		Properties props = super.props();
		props.setProperty("group.id", groupId);
		props.setProperty("zookeeper.connection.timeout.ms", Long.toString(zookeeperConnectionTimeoutMs));
		props.setProperty("zookeeper.session.timeout.ms", Long.toString(zookeeperSessionTimeoutMs));
		props.setProperty("zookeeper.sync.time.ms", Long.toString(zookeeperSyncTimeMs));
		props.setProperty("socket.timeout.ms", Long.toString(sessionTimeoutMs));
		props.setProperty("fetch.wait.max.ms", Long.toString(fetchWaitTimeoutMs));
		props.setProperty("consumer.timeout.ms", Long.toString(fetchWaitTimeoutMs));
		props.setProperty("rebalance.backoff.ms", Long.toString(backoffMs));
		props.setProperty("rebalance.max.retries", Integer.toString(rebalanceRetries));
		props.setProperty("auto.commit.interval.ms", Long.toString(autoCommitIntervalMs));

		props.setProperty("auto.commit.enable", Boolean.toString(autoCommitEnable));

		props.setProperty("socket.receive.buffer.bytes", Long.toString(transferBufferBytes));
		props.setProperty("fetch.message.max.bytes", Long.toString(fetchMessageMaxBytes));

		props.setProperty("auto.offset.reset", autoOffsetReset);
		props.setProperty("partition.assignment.strategy", partitionAssignmentStrategy);

		return props;
	}

	@Override
	public String toString() {
		return groupId + "@" + zookeeperConnect;
	}

	public String getGroupId() {
		return groupId;
	}
}
