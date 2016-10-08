package net.butfly.albatis.kafka.config;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.logger.Logger;

public class KafkaInputConfig extends KafkaConfigBase {
	private static final long serialVersionUID = -3028341800709486625L;
	private static final Logger logger = Logger.getLogger(KafkaInputConfig.class);
	protected long zookeeperSyncTimeMs;
	protected String groupId;
	protected boolean autoCommitEnable;
	protected long autoCommitIntervalMs;
	protected String autoOffsetReset;
	protected long sessionTimeoutMs;
	protected String partitionAssignmentStrategy;
	protected long fetchMessageMaxBytes;
	protected long fetchWaitTimeoutMs;

	public KafkaInputConfig(String classpathResourceName) {
		this(IOs.loadAsProps(classpathResourceName));
	}

	public KafkaInputConfig(Properties props) {
		super(props);
		groupId = null;
		if (props.containsKey("albatis.kafka.group.id")) groupId = props.getProperty("albatis.kafka.group.id");
		if (groupId == null || "".equals(groupId)) groupId = Systems.getMainClass().getSimpleName();
		if (Systems.isDebug()) {
			String suffix = "-DEBUG-" + new SimpleDateFormat("yyyyMMdd").format(new Date());
			groupId += suffix;
			logger.warn("Debug mode, suffix [" + suffix + "] append to groupId, now: [" + groupId + "].");
		}

		zookeeperSyncTimeMs = Long.valueOf(props.getProperty("albatis.kafka.zookeeper.sync.time.ms", "5000"));
		autoCommitEnable = Boolean.valueOf(props.getProperty("albatis.kafka.auto.commit.enable", "false"));
		autoCommitIntervalMs = Long.valueOf(props.getProperty("albatis.kafka.auto.commit.interval.ms", "1000"));
		autoOffsetReset = props.getProperty("albatis.kafka.auto.offset.reset", "smallest");
		sessionTimeoutMs = Long.valueOf(props.getProperty("albatis.kafka.session.timeout.ms", "30000"));
		fetchWaitTimeoutMs = Long.valueOf(props.getProperty("albatis.kafka.fetch.wait.timeout.ms", "5000"));
		partitionAssignmentStrategy = props.getProperty("albatis.kafka.partition.assignment.strategy", "range");
		fetchMessageMaxBytes = Long.valueOf(props.getProperty("albatis.kafka.fetch.message.max.bytes", "3145728"));
	}

	public ConsumerConfig getConfig() throws ConfigException {
		if (zookeeperConnect == null || groupId == null) throw new ConfigException(
				"Kafka configuration has no zookeeper and group definition.");
		return new ConsumerConfig(props());
	}

	@Override
	protected Properties props() {
		Properties props = super.props();
		props.setProperty("group.id", groupId);

		props.setProperty("zookeeper.connection.timeout.ms", Long.toString(zookeeperConnectionTimeoutMs));
		props.setProperty("zookeeper.sync.time.ms", Long.toString(zookeeperSyncTimeMs));
		props.setProperty("auto.commit.enable", Boolean.toString(autoCommitEnable));
		props.setProperty("auto.commit.interval.ms", Long.toString(autoCommitIntervalMs));
		props.setProperty("auto.offset.reset", autoOffsetReset);
		props.setProperty("socket.timeout.ms", Long.toString(sessionTimeoutMs));
		props.setProperty("fetch.wait.max.ms", Long.toString(fetchWaitTimeoutMs));
		props.setProperty("partition.assignment.strategy", partitionAssignmentStrategy);
		props.setProperty("socket.receive.buffer.bytes", Long.toString(transferBufferBytes));
		props.setProperty("fetch.message.max.bytes", Long.toString(fetchMessageMaxBytes));
		return props;
	}

	@Override
	public String toString() {
		return this.zookeeperConnect + "@" + this.groupId;
	}
}
