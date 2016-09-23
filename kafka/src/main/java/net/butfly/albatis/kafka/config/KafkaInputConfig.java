package net.butfly.albatis.kafka.config;

import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import net.butfly.albatis.kafka.KafkaException;

public class KafkaInputConfig extends KafkaConfigBase {
	private static final long serialVersionUID = -3028341800709486625L;
	public static final Properties DEFAULT_CONFIG = defaults();
	protected int zookeeperSyncTimeMs;
	protected String groupId;
	protected boolean autoCommitEnable;
	protected int autoCommitIntervalMs;
	protected String autoOffsetReset;
	protected int sessionTimeoutMs;
	protected String partitionAssignmentStrategy;
	protected int fetchMessageMaxBytes;

	public KafkaInputConfig(String zookeeperConnect, String groupId) {
		this(DEFAULT_CONFIG);
		this.zookeeperConnect = zookeeperConnect;
		this.groupId = groupId;
	}

	public KafkaInputConfig(String classpathResourceName) {
		super(classpathResourceName);
	}

	public KafkaInputConfig(Properties props) {
		super(props);
		zookeeperSyncTimeMs = Integer.valueOf(props.getProperty("zookeeper.sync.time.ms", "5000"));
		groupId = props.getProperty("group.id");
		autoCommitEnable = Boolean.valueOf(props.getProperty("auto.commit.enable", "false"));
		autoCommitIntervalMs = Integer.valueOf(props.getProperty("auto.commit.interval.ms", "1000"));
		autoOffsetReset = props.getProperty("auto.offset.reset", "smallest");
		sessionTimeoutMs = Integer.valueOf(props.getProperty("session.timeout.ms", "30000"));
		partitionAssignmentStrategy = props.getProperty("partition.assignment.strategy", "range");
		fetchMessageMaxBytes = Integer.valueOf(props.getProperty("fetch.message.max.bytes", "3145728"));
	}

	public ConsumerConfig getConfig() throws KafkaException {
		if (zookeeperConnect == null || groupId == null) throw new KafkaException(
				"Kafka configuration has no zookeeper and group definition.");
		return new ConsumerConfig(getProps());
	}

	@Override
	protected Properties getProps() {
		Properties props = super.getProps();
		props.setProperty("zookeeper.connection.timeout.ms", Integer.toString(zookeeperConnectionTimeoutMs));
		props.setProperty("zookeeper.sync.time.ms", Integer.toString(zookeeperSyncTimeMs));
		props.setProperty("group.id", groupId);
		props.setProperty("auto.commit.enable", Boolean.toString(autoCommitEnable));
		props.setProperty("auto.commit.interval.ms", Integer.toString(autoCommitIntervalMs));
		props.setProperty("auto.offset.reset", autoOffsetReset);
		props.setProperty("session.timeout.ms", Integer.toString(sessionTimeoutMs));
		props.setProperty("partition.assignment.strategy", partitionAssignmentStrategy);
		props.setProperty("transfer.buffer.bytes", Integer.toString(transferBufferBytes));
		props.setProperty("fetch.message.max.bytes", Integer.toString(fetchMessageMaxBytes));
		return props;
	}

	protected static Properties defaults() {
		Properties props = new Properties();
		props.setProperty("zookeeper.connection.timeout.ms", "15000");
		props.setProperty("transfer.buffer.bytes", "5242880");

		props.setProperty("zookeeper.sync.time.ms", "5000");
		props.setProperty("auto.commit.enable", "false");
		props.setProperty("auto.commit.interval.ms", "1000");
		props.setProperty("auto.offset.reset", "smallest");
		props.setProperty("session.timeout.ms", "30000");
		props.setProperty("partition.assignment.strategy", "range");
		props.setProperty("fetch.message.max.bytes", "3145728");
		return props;
	}
}
