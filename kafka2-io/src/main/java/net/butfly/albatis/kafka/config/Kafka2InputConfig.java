package net.butfly.albatis.kafka.config;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.kafka.Kafka2Input;

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
public class Kafka2InputConfig extends KafkaConfigBase {
	private static final long serialVersionUID = -3028341800709486625L;
	protected static final Logger logger = Logger.getLogger(Kafka2Input.class);
	private static final String DEFAULT_AUTO_COMMIT_MS = "60000";

	protected final Long zookeeperSyncTimeMs;
	protected final String consumerId;
	protected final String groupId;
	protected final Boolean autoCommitEnable;
	protected final Long autoCommitIntervalMs;
	protected final String autoOffsetReset;
	protected final Long sessionTimeoutMs;
	protected final String partitionAssignmentStrategy;
	protected final Long fetchMessageMaxBytes;
	protected Long fetchWaitTimeoutMs;
	protected final Integer rebalanceRetries;
	protected final Long zookeeperSessionTimeoutMs;

	// not for kafka, for albatis
	private int partitionParallelism;

	public Kafka2InputConfig(String consumerId, URISpec uri) {
		super(uri);
		this.consumerId = consumerId;
		groupId = calcGroupId(uri.getUsername());
		Map<String, String> props = uri.getParameters();

		zookeeperSyncTimeMs = props.containsKey("zksynctime") ? Long.parseLong(props.get("zksynctime")) : null;
		autoCommitIntervalMs = props.containsKey("autocommit") ? Long.parseLong(props.get("autocommit")) : null;
		autoCommitEnable = null != autoCommitIntervalMs && autoCommitIntervalMs > 0;
		autoOffsetReset = props.get("autoreset");
		sessionTimeoutMs = props.containsKey("sessiontimeout") ? Long.parseLong(props.get("sessiontimeout")) : null;
		fetchWaitTimeoutMs = props.containsKey("fetchtimeout") ? Long.parseLong(props.get("fetchtimeout")) : null;
		partitionAssignmentStrategy = props.get("strategy");
		fetchMessageMaxBytes = props.containsKey("fetchmax") ? Long.parseLong(props.get("fetchmax")) : null;
		rebalanceRetries = props.containsKey("rebalanceretries") ? Integer.parseInt(props.get("rebalanceretries")) : null;
		zookeeperSessionTimeoutMs = props.containsKey("zksessiontimeout") ? Long.parseLong(props.get("zksessiontimeout")) : null;

		partitionParallelism = props.containsKey("parallelism") ? Integer.parseInt(props.get("parallelism")) : -1;
	}

	/**
	 * @deprecated use {@link URISpec} to construct kafka configuration.
	 */
	@Deprecated
	public Kafka2InputConfig(String consumerId, Properties props) {
		super(props);
		this.consumerId = consumerId;
		groupId = calcGroupId(props.getProperty("albatis.kafka.group.id"));

		zookeeperSyncTimeMs = props.containsKey(PROP_PREFIX + "zookeeper.sync.time.ms") ? //
				Long.parseLong(props.getProperty(PROP_PREFIX + "zookeeper.sync.time.ms")) : null;
		autoCommitIntervalMs = props.containsKey(PROP_PREFIX + "auto.commit.interval.ms") ? //
				Long.parseLong(props.getProperty(PROP_PREFIX + "auto.commit.interval.ms", DEFAULT_AUTO_COMMIT_MS)) : null;
		autoCommitEnable = props.containsKey(PROP_PREFIX + "auto.commit.enable") ? //
				Boolean.parseBoolean(props.getProperty(PROP_PREFIX + "auto.commit.enable", Boolean.toString(autoCommitIntervalMs > 0))) : null;
		autoOffsetReset = props.getProperty(PROP_PREFIX + "auto.offset.reset");
		sessionTimeoutMs = props.containsKey(PROP_PREFIX + "session.timeout.ms") ? //
				Long.parseLong(props.getProperty(PROP_PREFIX + "session.timeout.ms")) : null;
		fetchWaitTimeoutMs = props.containsKey(PROP_PREFIX + "fetch.wait.timeout.ms") ? //
				Long.parseLong(props.getProperty(PROP_PREFIX + "fetch.wait.timeout.ms")) : null;
		partitionAssignmentStrategy = props.getProperty(PROP_PREFIX + "partition.assignment.strategy");
		fetchMessageMaxBytes = props.containsKey(PROP_PREFIX + "fetch.message.max.bytes") ? //
				Long.parseLong(props.getProperty(PROP_PREFIX + "fetch.message.max.bytes")) : null;
		rebalanceRetries = props.containsKey(PROP_PREFIX + "rebalance.retries") ? //
				Integer.parseInt(props.getProperty(PROP_PREFIX + "rebalance.retries")) : null;
		zookeeperSessionTimeoutMs = props.containsKey(PROP_PREFIX + "zookeeper.session.timeout.ms") ? //
				Long.parseLong(props.getProperty(PROP_PREFIX + "zookeeper.session.timeout.ms")) : null;

		partitionParallelism = props.containsKey(PROP_PREFIX + "partition.parallelism") ? //
				Integer.parseInt(props.getProperty(PROP_PREFIX + "partition.parallelism")) : null;
	}

	private static final boolean RANDOM_SUFFIX = Boolean.parseBoolean(Configs.gets("albatis.kafka.from.begin.by.groupid.suffix", "false"));

	private static String calcGroupId(String configGroupId) {
		String origin = configGroupId;
		if (configGroupId == null || "".equals(configGroupId)) configGroupId = Systems.getMainClass().getSimpleName();
		configGroupId = Systems.suffixDebug(configGroupId, logger);
		if (RANDOM_SUFFIX) configGroupId += UUID.randomUUID();
		if (!configGroupId.equals(origin)) logger.info("Kafka group id adjust into: " + configGroupId);
		return configGroupId;
	}

	public int getDefaultPartitionParallelism() { return partitionParallelism; }

	@Override
	public Properties props() {
		Properties props = super.props();
		props.setProperty("group.id", groupId);
		if (null != zookeeperConnectionTimeoutMs) props.setProperty("zookeeper.connection.timeout.ms", //
				Long.toString(zookeeperConnectionTimeoutMs));
		if (null != zookeeperSessionTimeoutMs) props.setProperty("zookeeper.session.timeout.ms", Long.toString(zookeeperSessionTimeoutMs));
		if (null != zookeeperSyncTimeMs) props.setProperty("zookeeper.sync.time.ms", Long.toString(zookeeperSyncTimeMs));
		if (null != sessionTimeoutMs) props.setProperty("socket.timeout.ms", Long.toString(sessionTimeoutMs));
		if (null != fetchWaitTimeoutMs) {
			props.setProperty("fetch.wait.max.ms", Long.toString(fetchWaitTimeoutMs));
			props.setProperty("consumer.timeout.ms", Long.toString(fetchWaitTimeoutMs));
		}
		if (null != backoffMs) {
			props.setProperty("rebalance.backoff.ms", Long.toString(backoffMs));
			props.setProperty("reconnect.backoff.ms", Long.toString(backoffMs));
		}
		if (null != rebalanceRetries) props.setProperty("rebalance.max.retries", Integer.toString(rebalanceRetries));
		if (null != autoCommitIntervalMs) props.setProperty("auto.commit.interval.ms", Long.toString(autoCommitIntervalMs));

		if (null != autoCommitEnable) props.setProperty("auto.commit.enable", Boolean.toString(autoCommitEnable));

		if (null != transferBufferBytes) props.setProperty("socket.receive.buffer.bytes", Long.toString(transferBufferBytes));
		if (null != fetchMessageMaxBytes) props.setProperty("fetch.message.max.bytes", Long.toString(fetchMessageMaxBytes));

		if (null != autoOffsetReset) props.setProperty("auto.offset.reset", autoOffsetReset);
		if (null != partitionAssignmentStrategy) props.setProperty("partition.assignment.strategy", partitionAssignmentStrategy);
		kerberosConfig(props);
		tbds(props);
		return props;
	}

	@Override
	public String toString() {
		return groupId + "@" + zookeeperConnect;
	}

	public String getGroupId() { return groupId; }

}
