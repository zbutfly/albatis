package net.butfly.albatis.kafka.config;

import java.io.Serializable;
import java.util.Properties;

import net.butfly.albacore.utils.Reflections;

public abstract class KafkaConfigBase implements Serializable {
	public long getPoolSize() {
		return poolSize;
	}

	public long getBatchSize() {
		return batchSize;
	}

	private static final long serialVersionUID = -4020530608706621876L;
	protected final String zookeeperConnect;
	protected final long zookeeperConnectionTimeoutMs;
	protected final long transferBufferBytes;

	protected long poolSize;
	protected long batchSize;

	public KafkaConfigBase(Properties props) {
		super();
		zookeeperConnect = props.getProperty("albatis.kafka.zookeeper");
		zookeeperConnectionTimeoutMs = Long.valueOf(props.getProperty("albatis.kafka.zookeeper.connection.timeout.ms", "15000"));
		transferBufferBytes = Long.valueOf(props.getProperty("albatis.kafka.transfer.buffer.bytes", "5242880"));
		poolSize = Long.parseLong(props.getProperty("albatis.kafka.pool.size", "1000"));
		batchSize = Long.parseLong(props.getProperty("albatis.kafka.batch.size", "100"));
	}

	public KafkaConfigBase(String classpathResourceName) {
		this(Reflections.loadAsProps(classpathResourceName));
	}

	protected Properties props() {
		Properties props = new Properties();
		props.setProperty("zookeeper.connect", zookeeperConnect);
		return props;
	}
}
