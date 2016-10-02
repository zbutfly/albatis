package net.butfly.albatis.kafka.config;

import java.io.Serializable;
import java.util.Properties;

import net.butfly.albacore.utils.IOs;

public abstract class KafkaConfigBase implements Serializable {
	private static final long serialVersionUID = -4020530608706621876L;
	protected final String zookeeperConnect;
	protected final long zookeeperConnectionTimeoutMs;
	protected final long transferBufferBytes;

	private final long poolSize;
	private final long batchSize;
	private final long statsMsgs;
	private final String queuePath;

	public long getPoolSize() {
		return poolSize;
	}

	public long getBatchSize() {
		return batchSize;
	}

	public String getQueuePath() {
		return queuePath;
	}

	public long getStatsMsgs() {
		return statsMsgs;
	}

	public KafkaConfigBase(Properties props) {
		super();
		zookeeperConnect = props.getProperty("albatis.kafka.zookeeper");
		zookeeperConnectionTimeoutMs = Long.valueOf(props.getProperty("albatis.kafka.zookeeper.connection.timeout.ms", "15000"));
		transferBufferBytes = Long.valueOf(props.getProperty("albatis.kafka.transfer.buffer.bytes", "5242880"));
		poolSize = Long.parseLong(props.getProperty("albatis.kafka.pool.size", "100000"));
		batchSize = Long.parseLong(props.getProperty("albatis.kafka.batch.size", "1000"));
		queuePath = props.getProperty("albatis.kafka.queue.path", "./local-queue");
		statsMsgs = Long.parseLong(props.getProperty("albatis.kafka.queue.stats.messages", "1000000"));
	}

	public KafkaConfigBase(String classpathResourceName) {
		this(IOs.loadAsProps(classpathResourceName));
	}

	protected Properties props() {
		Properties props = new Properties();
		props.setProperty("zookeeper.connect", zookeeperConnect);
		return props;
	}
}
