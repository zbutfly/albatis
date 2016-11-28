package net.butfly.albatis.kafka.config;

import java.io.IOException;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URIs;
import net.butfly.albacore.io.URIs.Schema;
import net.butfly.albacore.utils.Configs;
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

	// not for kafka, for albatis
	private boolean parallelismEnable;

	public KafkaInputConfig(String uri) throws IOException {
		this(Configs.read(URIs.open(uri, Schema.FILE, Schema.CLASSPATH, Schema.ZOOKEEPER)));
	}

	public KafkaInputConfig(Properties props) {
		super(props);
		groupId = null;
		if (props.containsKey("albatis.kafka.group.id")) groupId = props.getProperty("albatis.kafka.group.id");
		if (groupId == null || "".equals(groupId)) groupId = Systems.getMainClass().getSimpleName();
		groupId = Systems.suffixDebug(groupId, logger);

		String v;
		v = props.getProperty("albatis.kafka.zookeeper.sync.time.ms", "5000");
		zookeeperSyncTimeMs = Long.parseLong(v.trim());
		v = props.getProperty("albatis.kafka.auto.commit.enable", "true");
		autoCommitEnable = Boolean.parseBoolean(v.trim());
		v = props.getProperty("albatis.kafka.auto.commit.interval.ms", "60000");
		autoCommitIntervalMs = Long.parseLong(v.trim());
		autoOffsetReset = props.getProperty("albatis.kafka.auto.offset.reset", "smallest");
		v = props.getProperty("albatis.kafka.session.timeout.ms", "30000");
		sessionTimeoutMs = Long.parseLong(v.trim());
		v = props.getProperty("albatis.kafka.fetch.wait.timeout.ms", "500");
		fetchWaitTimeoutMs = Long.parseLong(v.trim());
		partitionAssignmentStrategy = props.getProperty("albatis.kafka.partition.assignment.strategy", "range");
		v = props.getProperty("albatis.kafka.fetch.message.max.bytes", "3145728");
		fetchMessageMaxBytes = Long.parseLong(v.trim());
		v = props.getProperty("albatis.kafka.parallelism.enable", "false");
		parallelismEnable = Boolean.parseBoolean(v.trim());
	}

	public boolean isParallelismEnable() {
		return parallelismEnable;
	}

	public ConsumerConfig getConfig() throws ConfigException {
		if (zookeeperConnect == null || groupId == null) throw new ConfigException(
				"Kafka configuration has no zookeeper and group definition.");
		return new ConsumerConfig(props());
	}

	@Override
	public Properties props() {
		Properties props = super.props();
		props.setProperty("group.id", groupId);
		props.setProperty("zookeeper.connection.timeout.ms", Long.toString(zookeeperConnectionTimeoutMs));
		props.setProperty("zookeeper.sync.time.ms", Long.toString(zookeeperSyncTimeMs));
		props.setProperty("auto.commit.enable", Boolean.toString(autoCommitEnable));
		props.setProperty("auto.commit.interval.ms", Long.toString(autoCommitIntervalMs));
		props.setProperty("auto.offset.reset", autoOffsetReset);
		props.setProperty("socket.timeout.ms", Long.toString(sessionTimeoutMs));
		props.setProperty("fetch.wait.max.ms", Long.toString(fetchWaitTimeoutMs));
		props.setProperty("consumer.timeout.ms", Long.toString(fetchWaitTimeoutMs));
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
