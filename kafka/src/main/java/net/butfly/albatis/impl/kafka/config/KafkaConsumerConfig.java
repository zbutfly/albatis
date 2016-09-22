package net.butfly.albatis.impl.kafka.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;

public class KafkaConsumerConfig implements Serializable {
	private static final long serialVersionUID = -3028341800709486625L;
	public static final Properties DEFAULT_CONFIG = defaults();
	private String zookeeperConnect;
	private int zookeeperConnectionTimeoutMs;
	private int zookeeperSyncTimeMs;
	private String groupId;
	private boolean autoCommitEnable;
	private int autoCommitIntervalMs;
	private String autoOffsetReset;
	private int sessionTimeoutMs;
	private String partitionAssignmentStrategy;
	private int socketReceiveBufferBytes;
	private int fetchMessageMaxBytes;

	public KafkaConsumerConfig(String zookeeperConnect, String groupId) {
		this(DEFAULT_CONFIG);
		this.zookeeperConnect = zookeeperConnect;
		this.groupId = groupId;
	}

	public KafkaConsumerConfig(String classpathResourceName) {
		Properties prop = new Properties();
		InputStream ips = Thread.currentThread().getContextClassLoader().getResourceAsStream(classpathResourceName);
		if (null == ips) throw new RuntimeException("Configuration not found: " + classpathResourceName);
		try {
			prop.load(ips);
		} catch (IOException e) {
			throw new RuntimeException("Configuration could not load: " + classpathResourceName, e);
		}
	}

	public KafkaConsumerConfig(Properties props) {
		super();
		zookeeperConnect = props.getProperty("zookeeper.connect");
		zookeeperConnectionTimeoutMs = Integer.valueOf(props.getProperty("zookeeper.connection.timeout.ms", "15000"));
		zookeeperSyncTimeMs = Integer.valueOf(props.getProperty("zookeeper.sync.time.ms", "5000"));
		groupId = props.getProperty("group.id");
		autoCommitEnable = Boolean.valueOf(props.getProperty("auto.commit.enable", "false"));
		autoCommitIntervalMs = Integer.valueOf(props.getProperty("auto.commit.interval.ms", "1000"));
		autoOffsetReset = props.getProperty("auto.offset.reset", "smallest");
		sessionTimeoutMs = Integer.valueOf(props.getProperty("session.timeout.ms", "30000"));
		partitionAssignmentStrategy = props.getProperty("partition.assignment.strategy", "range");
		socketReceiveBufferBytes = Integer.valueOf(props.getProperty("socket.receive.buffer.bytes", "5242880"));
		fetchMessageMaxBytes = Integer.valueOf(props.getProperty("fetch.message.max.bytes", "3145728"));
	}

	public ConsumerConfig getConfig() {
		Properties props = new Properties();
		props.setProperty("zookeeper.connect", zookeeperConnect);
		props.setProperty("zookeeper.connection.timeout.ms", Integer.toString(zookeeperConnectionTimeoutMs));
		props.setProperty("zookeeper.sync.time.ms", Integer.toString(zookeeperSyncTimeMs));
		props.setProperty("group.id", groupId);
		props.setProperty("auto.commit.enable", Boolean.toString(autoCommitEnable));
		props.setProperty("auto.commit.interval.ms", Integer.toString(autoCommitIntervalMs));
		props.setProperty("auto.offset.reset", autoOffsetReset);
		props.setProperty("session.timeout.ms", Integer.toString(sessionTimeoutMs));
		props.setProperty("partition.assignment.strategy", partitionAssignmentStrategy);
		props.setProperty("socket.receive.buffer.bytes", Integer.toString(socketReceiveBufferBytes));
		props.setProperty("fetch.message.max.bytes", Integer.toString(fetchMessageMaxBytes));
		return new ConsumerConfig(props);
	}

	private static Properties defaults() {
		Properties props = new Properties();
		props.setProperty("zookeeper.connection.timeout.ms", Integer.toString(15000));
		props.setProperty("zookeeper.sync.time.ms", Integer.toString(5000));
		props.setProperty("auto.commit.enable", Boolean.toString(false));
		props.setProperty("auto.commit.interval.ms", Integer.toString(1000));
		props.setProperty("auto.offset.reset", "smallest");
		props.setProperty("session.timeout.ms", Integer.toString(30000));
		props.setProperty("partition.assignment.strategy", "range");
		props.setProperty("socket.receive.buffer.bytes", Integer.toString(5242880));
		props.setProperty("fetch.message.max.bytes", Integer.toString(3145728));
		return props;
	}

	public String getZookeeperConnect() {
		return zookeeperConnect;
	}

	public void setZookeeperConnect(String zookeeperConnect) {
		this.zookeeperConnect = zookeeperConnect;
	}

	public int getZookeeperConnectionTimeoutMs() {
		return zookeeperConnectionTimeoutMs;
	}

	public void setZookeeperConnectionTimeoutMs(int zookeeperConnectionTimeoutMs) {
		this.zookeeperConnectionTimeoutMs = zookeeperConnectionTimeoutMs;
	}

	public int getZookeeperSyncTimeMs() {
		return zookeeperSyncTimeMs;
	}

	public void setZookeeperSyncTimeMs(int zookeeperSyncTimeMs) {
		this.zookeeperSyncTimeMs = zookeeperSyncTimeMs;
	}

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public boolean isAutoCommitEnable() {
		return autoCommitEnable;
	}

	public void setAutoCommitEnable(boolean autoCommitEnable) {
		this.autoCommitEnable = autoCommitEnable;
	}

	public int getAutoCommitIntervalMs() {
		return autoCommitIntervalMs;
	}

	public void setAutoCommitIntervalMs(int autoCommitIntervalMs) {
		this.autoCommitIntervalMs = autoCommitIntervalMs;
	}

	public String getAutoOffsetReset() {
		return autoOffsetReset;
	}

	public void setAutoOffsetReset(String autoOffsetReset) {
		this.autoOffsetReset = autoOffsetReset;
	}

	public int getSessionTimeoutMs() {
		return sessionTimeoutMs;
	}

	public void setSessionTimeoutMs(int sessionTimeoutMs) {
		this.sessionTimeoutMs = sessionTimeoutMs;
	}

	public String getPartitionAssignmentStrategy() {
		return partitionAssignmentStrategy;
	}

	public void setPartitionAssignmentStrategy(String partitionAssignmentStrategy) {
		this.partitionAssignmentStrategy = partitionAssignmentStrategy;
	}

	public int getSocketReceiveBufferBytes() {
		return socketReceiveBufferBytes;
	}

	public void setSocketReceiveBufferBytes(int socketReceiveBufferBytes) {
		this.socketReceiveBufferBytes = socketReceiveBufferBytes;
	}

	public int getFetchMessageMaxBytes() {
		return fetchMessageMaxBytes;
	}

	public void setFetchMessageMaxBytes(int fetchMessageMaxBytes) {
		this.fetchMessageMaxBytes = fetchMessageMaxBytes;
	}
}
