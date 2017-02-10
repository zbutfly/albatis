package net.butfly.albatis.kafka.config;

import java.io.IOException;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.URIs;
import net.butfly.albacore.io.URIs.Schema;
import net.butfly.albacore.utils.Configs;
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
	protected String groupId;
	protected boolean autoCommitEnable;
	protected long autoCommitIntervalMs;
	protected String autoOffsetReset;
	protected long sessionTimeoutMs;
	protected String partitionAssignmentStrategy;
	protected long fetchMessageMaxBytes;
	protected long fetchWaitTimeoutMs;
	protected long rebalanceBackoffMs;
	protected int rebalanceRetries;
	protected long zookeeperSessionTimeoutMs;

	// not for kafka, for albatis
	private int partitionParallelism;

	/**
	 * @deprecated use {@link URISpec} to construct kafka configuration.
	 */
	@Deprecated
	public KafkaInputConfig(String uri) throws IOException {
		this(Configs.read(URIs.open(uri, Schema.FILE, Schema.CLASSPATH, Schema.ZOOKEEPER)));
	}

	public KafkaInputConfig(URISpec uri) {
		super(uri);
		groupId = uri.getUsername();
		if (groupId == null || "".equals(groupId)) groupId = Systems.suffixDebug(Systems.getMainClass().getSimpleName(), logger);
		Properties props = uri.getParameters();

		zookeeperSyncTimeMs = Long.parseLong(props.getProperty("zksynctime", "15000").trim());
		autoCommitIntervalMs = Long.parseLong(props.getProperty("autocommit", DEFAULT_AUTO_COMMIT_MS).trim());
		autoCommitEnable = autoCommitIntervalMs > 0;
		autoOffsetReset = props.getProperty("autoreset", "smallest");
		sessionTimeoutMs = Long.parseLong(props.getProperty("sessiontimeout", "30000").trim());
		fetchWaitTimeoutMs = Long.parseLong(props.getProperty("fetchtimeout", "500").trim());
		partitionAssignmentStrategy = props.getProperty("strategy", "range");
		fetchMessageMaxBytes = Long.parseLong(props.getProperty("fetchmax", "10485760").trim());
		rebalanceBackoffMs = Long.parseLong(props.getProperty("rebalancetime", "10000"));
		rebalanceRetries = Integer.parseInt(props.getProperty("rebalanceretries", "2"));
		zookeeperSessionTimeoutMs = Long.parseLong(props.getProperty("zksessiontimeout", "30000"));

		partitionParallelism = Integer.parseInt(props.getProperty("parallelism", "0").trim());
	}

	/**
	 * @deprecated use {@link URISpec} to construct kafka configuration.
	 */
	@Deprecated
	public KafkaInputConfig(Properties props) {
		super(props);
		groupId = null;
		if (props.containsKey("albatis.kafka.group.id")) groupId = props.getProperty("albatis.kafka.group.id");
		if (groupId == null || "".equals(groupId)) groupId = Systems.getMainClass().getSimpleName();
		groupId = Systems.suffixDebug(groupId, logger);

		zookeeperSyncTimeMs = Long.parseLong(props.getProperty("albatis.kafka.zookeeper.sync.time.ms", "15000").trim());
		autoCommitIntervalMs = Long.parseLong(props.getProperty("albatis.kafka.auto.commit.interval.ms", DEFAULT_AUTO_COMMIT_MS).trim());
		autoCommitEnable = Boolean.parseBoolean(props.getProperty("albatis.kafka.auto.commit.enable", Boolean.toString(
				autoCommitIntervalMs > 0)).trim());
		autoOffsetReset = props.getProperty("albatis.kafka.auto.offset.reset", "smallest");
		sessionTimeoutMs = Long.parseLong(props.getProperty("albatis.kafka.session.timeout.ms", "30000").trim());
		fetchWaitTimeoutMs = Long.parseLong(props.getProperty("albatis.kafka.fetch.wait.timeout.ms", "500").trim());
		partitionAssignmentStrategy = props.getProperty("albatis.kafka.partition.assignment.strategy", "range");
		fetchMessageMaxBytes = Long.parseLong(props.getProperty("albatis.kafka.fetch.message.max.bytes", "10485760").trim());
		rebalanceBackoffMs = Long.parseLong(props.getProperty("albatis.kafka.rebalance.backoff.ms", "10000"));
		rebalanceRetries = Integer.parseInt(props.getProperty("albatis.kafka.rebalance.retries", "2"));
		zookeeperSessionTimeoutMs = Long.parseLong(props.getProperty("albatis.kafka.zookeeper.session.timeout.ms", "30000"));

		partitionParallelism = Integer.parseInt(props.getProperty("albatis.kafka.partition.parallelism", "0").trim());
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
		props.setProperty("rebalance.backoff.ms", Long.toString(rebalanceBackoffMs));
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
