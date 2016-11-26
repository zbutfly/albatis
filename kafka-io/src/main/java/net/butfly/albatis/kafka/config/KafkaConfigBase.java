package net.butfly.albatis.kafka.config;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import org.apache.kafka.common.serialization.ByteArraySerializer;

import net.butfly.albacore.utils.Configs;

public abstract class KafkaConfigBase implements Serializable {
	private static final long serialVersionUID = -4020530608706621876L;
	protected final String zookeeperConnect;
	protected String bootstrapServers;
	protected final long zookeeperConnectionTimeoutMs;
	protected final long transferBufferBytes;
	protected String keySerializerClass;
	protected String valueSerializerClass;

	private final long poolSize;

	public long getPoolSize() {
		return poolSize;
	}

	public KafkaConfigBase(Properties props) {
		super();
		zookeeperConnect = props.getProperty("albatis.kafka.zookeeper");
		bootstrapServers = props.getProperty("albatis.kafka.bootstrap.servers");
		zookeeperConnectionTimeoutMs = Long.valueOf(props.getProperty("albatis.kafka.zookeeper.connection.timeout.ms", "15000"));
		transferBufferBytes = Long.valueOf(props.getProperty("albatis.kafka.transfer.buffer.bytes", "5242880"));
		keySerializerClass = props.getProperty("albatis.kafka.key.serializer.class", ByteArraySerializer.class.getName());
		valueSerializerClass = props.getProperty("albatis.kafka.value.serializer.class", ByteArraySerializer.class.getName());
		poolSize = Long.parseLong(props.getProperty("albatis.kafka.pool.size", "100000"));
	}

	public KafkaConfigBase(String classpathResourceName) throws IOException {
		this(Configs.read(classpathResourceName));
	}

	public Properties props() {
		Properties props = new Properties();
		props.setProperty("zookeeper.connect", zookeeperConnect);
		props.setProperty("bootstrap.servers", bootstrapServers);
		props.setProperty("key.serializer", keySerializerClass);
		props.setProperty("value.serializer", valueSerializerClass);
		return props;
	}
}
