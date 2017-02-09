package net.butfly.albatis.kafka.config;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import com.google.common.base.Joiner;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.kafka.ZKConn;

public abstract class KafkaConfigBase implements Serializable {
	private static final long serialVersionUID = -4020530608706621876L;
	private static final String PROP_PREFIX = "albatis.kafka.";
	static final Logger logger = Logger.getLogger(KafkaConfigBase.class);
	protected final String zookeeperConnect;
	protected String bootstrapServers;
	protected final long zookeeperConnectionTimeoutMs;
	protected final long transferBufferBytes;
	protected String keySerializerClass;
	protected String valueSerializerClass;

	private final long poolSize;
	private final String[] topics;

	public long getPoolSize() {
		return poolSize;
	}

	/**
	 * @deprecated use {@link URISpec} to construct kafka configuration.
	 */
	@Deprecated
	public KafkaConfigBase(Properties props) {
		super();
		zookeeperConnect = props.getProperty(PROP_PREFIX + "zookeeper");
		bootstrapServers = props.getProperty(PROP_PREFIX + "bootstrap.servers");
		if (zookeeperConnect != null) {
			if (bootstrapServers != null) logger.warn("Zookeeper detect broken list automatically, configured [" + PROP_PREFIX
					+ "bootstrap.servers] is not used (current value: [" + bootstrapServers + "])");
			try (ZKConn zk = new ZKConn(zookeeperConnect)) {
				bootstrapServers = Joiner.on(",").join(zk.getBorkers());
				logger.info("Zookeeper detect broken list automatically: [" + bootstrapServers + "])");
			} catch (Exception e) {
				bootstrapServers = null;
				logger.warn("Zookeeper detect broken list failure", e);
			}
		} else if (bootstrapServers == null) throw new ConfigException("Neither [" + PROP_PREFIX + "zookeeper] nor [" + PROP_PREFIX
				+ "bootstrap.servers] found");
		zookeeperConnectionTimeoutMs = Long.valueOf(props.getProperty(PROP_PREFIX + "zookeeper.connection.timeout.ms", "20000"));
		transferBufferBytes = Long.valueOf(props.getProperty(PROP_PREFIX + "transfer.buffer.bytes", "5242880"));
		keySerializerClass = props.getProperty(PROP_PREFIX + "key.serializer.class", ByteArraySerializer.class.getName());
		valueSerializerClass = props.getProperty(PROP_PREFIX + "value.serializer.class", ByteArraySerializer.class.getName());
		poolSize = Long.parseLong(props.getProperty(PROP_PREFIX + "internal.pool.size", "3000000"));
		String ts = props.getProperty(PROP_PREFIX + "topics");
		topics = null == ts ? new String[0] : ts.split(",");
	}

	public KafkaConfigBase(URISpec uri) {
		super();
		String sch = uri.getScheme();
		if (sch.startsWith("kafka:")) sch = sch.substring(6);
		switch (sch) {
		case "zk":
		case "zookeeper":
		case "kafka":
			zookeeperConnect = uri.getHost() + uri.getPath();
			try (ZKConn zk = new ZKConn(zookeeperConnect)) {
				bootstrapServers = Joiner.on(",").join(zk.getBorkers());
				logger.debug("Zookeeper detect broken list automatically: [" + bootstrapServers + "])");
			} catch (Exception e) {
				bootstrapServers = null;
				logger.warn("Zookeeper detect broken list failure", e);
			}
			break;
		case "bootstrap":
			bootstrapServers = uri.getHost();
			zookeeperConnect = null;
			break;
		default:
			throw new ConfigException("Neither [zk] nor [bootstrap.servers] found");
		}
		Properties props = uri.getParameters();
		zookeeperConnectionTimeoutMs = Long.valueOf(props.getProperty("zkconntimeout", "20000"));
		transferBufferBytes = Long.valueOf(props.getProperty("buffer", "5242880"));
		keySerializerClass = props.getProperty("kserial", ByteArraySerializer.class.getName());
		valueSerializerClass = props.getProperty("vserial", ByteArraySerializer.class.getName());
		poolSize = Long.parseLong(props.getProperty("pool", "3000000"));
		String ts = props.getProperty("topics");
		topics = null == ts ? new String[0] : ts.split(",");
	}

	/**
	 * @deprecated use {@link URISpec} to construct kafka configuration.
	 */
	@Deprecated
	public KafkaConfigBase(String classpathResourceName) throws IOException {
		this(Configs.read(classpathResourceName));
	}

	public Properties props() {
		Properties props = new Properties();
		if (null != zookeeperConnect) props.setProperty("zookeeper.connect", zookeeperConnect);
		if (null != bootstrapServers) props.setProperty("bootstrap.servers", bootstrapServers);
		if (null != keySerializerClass) props.setProperty("key.serializer", keySerializerClass);
		if (null != valueSerializerClass) props.setProperty("value.serializer", valueSerializerClass);
		return props;
	}

	public String getZookeeperConnect() {
		return zookeeperConnect;
	}

	public String[] getTopics() {
		return topics;
	}
}
