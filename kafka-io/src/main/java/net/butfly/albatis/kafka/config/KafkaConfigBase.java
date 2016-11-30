package net.butfly.albatis.kafka.config;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import com.google.common.base.Joiner;

import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.logger.Logger;

public abstract class KafkaConfigBase implements Serializable {
	private static final long serialVersionUID = -4020530608706621876L;
	static final Logger logger = Logger.getLogger(KafkaConfigBase.class);
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
		if (zookeeperConnect != null) {
			if (bootstrapServers != null) logger.warn(
					"Zookeeper detect broken list automatically, configured [albatis.kafka.bootstrap.servers] is not used (current value: ["
							+ bootstrapServers + "])");
			bootstrapServers = Joiner.on(",").join(Kafkas.getBorkers(zookeeperConnect));
			logger.info("Zookeeper detect broken list automatically: [" + bootstrapServers + "])");
		} else if (bootstrapServers == null) throw new ConfigException(
				"Neither [albatis.kafka.zookeeper] nor [albatis.kafka.bootstrap.servers] found");
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

	public String getZookeeperConnect() {
		return zookeeperConnect;
	}
}
