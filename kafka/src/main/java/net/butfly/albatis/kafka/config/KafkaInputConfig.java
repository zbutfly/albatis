package net.butfly.albatis.kafka.config;

import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albatis.kafka.KafkaException;

public class KafkaInputConfig extends KafkaConfigBase {
	private static final long serialVersionUID = -3028341800709486625L;
	public static final Properties DEFAULT_CONFIG = defaults();
	protected long zookeeperSyncTimeMs;
	protected String groupId;
	protected boolean autoCommitEnable;
	protected long autoCommitIntervalMs;
	protected String autoOffsetReset;
	protected long sessionTimeoutMs;
	protected String partitionAssignmentStrategy;
	protected long fetchMessageMaxBytes;

	public KafkaInputConfig(String classpathResourceName) {
		this(Reflections.loadAsProps(classpathResourceName));
	}

	public KafkaInputConfig(Properties props) {
		super(props);
		groupId = props.getProperty("albatis.kafka.group.id");

		zookeeperSyncTimeMs = Integer.valueOf(props.getProperty("albatis.kafka.zookeeper.sync.time.ms", "5000"));
		autoCommitEnable = Boolean.valueOf(props.getProperty("albatis.kafka.auto.commit.enable", "false"));
		autoCommitIntervalMs = Integer.valueOf(props.getProperty("albatis.kafka.auto.commit.interval.ms", "1000"));
		autoOffsetReset = props.getProperty("albatis.kafka.auto.offset.reset", "smallest");
		sessionTimeoutMs = Integer.valueOf(props.getProperty("albatis.kafka.session.timeout.ms", "30000"));
		partitionAssignmentStrategy = props.getProperty("albatis.kafka.partition.assignment.strategy", "range");
		fetchMessageMaxBytes = Integer.valueOf(props.getProperty("albatis.kafka.fetch.message.max.bytes", "3145728"));
	}

	public ConsumerConfig getConfig() throws KafkaException {
		if (zookeeperConnect == null || groupId == null) throw new KafkaException(
				"Kafka configuration has no zookeeper and group definition.");
		return new ConsumerConfig(props());
	}

	@Override
	protected Properties props() {
		Properties props = super.props();
		props.setProperty("albatis.kafka.zookeeper.connection.timeout.ms", Long.toString(zookeeperConnectionTimeoutMs));
		props.setProperty("zookeeper.sync.time.ms", Long.toString(zookeeperSyncTimeMs));
		props.setProperty("group.id", groupId);
		props.setProperty("albatis.kafka.auto.commit.enable", Boolean.toString(autoCommitEnable));
		props.setProperty("auto.commit.interval.ms", Long.toString(autoCommitIntervalMs));
		props.setProperty("auto.offset.reset", autoOffsetReset);
		props.setProperty("session.timeout.ms", Long.toString(sessionTimeoutMs));
		props.setProperty("partition.assignment.strategy", partitionAssignmentStrategy);
		props.setProperty("albatis.kafka.transfer.buffer.bytes", Long.toString(transferBufferBytes));
		props.setProperty("fetch.message.max.bytes", Long.toString(fetchMessageMaxBytes));
		return props;
	}

	protected static Properties defaults() {
		Properties props = new Properties();
		props.setProperty("albatis.kafka.zookeeper.connection.timeout.ms", "15000");
		props.setProperty("albatis.kafka.transfer.buffer.bytes", "5242880");

		props.setProperty("zookeeper.sync.time.ms", "5000");
		props.setProperty("albatis.kafka.auto.commit.enable", "false");
		props.setProperty("auto.commit.interval.ms", "1000");
		props.setProperty("auto.offset.reset", "smallest");
		props.setProperty("session.timeout.ms", "30000");
		props.setProperty("partition.assignment.strategy", "range");
		props.setProperty("fetch.message.max.bytes", "3145728");
		return props;
	}
}
