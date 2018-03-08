package net.butfly.albatis.kafka.config;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import com.google.common.base.Joiner;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.logger.Logger;

public abstract class KafkaConfigBase implements Serializable {
	private static final long serialVersionUID = -4020530608706621876L;
	protected static final String PROP_PREFIX = "albatis.kafka.";
	static final Logger logger = Logger.getLogger(KafkaConfigBase.class);
	protected final String zookeeperConnect;
	protected String bootstrapServers;
	final protected long backoffMs; // input: rebalance, output: retry
	protected final long zookeeperConnectionTimeoutMs;
	protected final long transferBufferBytes;
	protected String keySerializerClass;
	protected String valueSerializerClass;

	private final long poolSize;
	protected final List<String> topics;

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
			try (KafkaZkParser zk = new KafkaZkParser(zookeeperConnect)) {
				bootstrapServers = Joiner.on(",").join(zk.getBrokers());
				logger.trace("Zookeeper detect broken list automatically: [" + bootstrapServers + "])");
			} catch (Exception e) {
				bootstrapServers = null;
				logger.warn("Zookeeper detect broken list failure", e);
			}
		} else if (bootstrapServers == null) throw new ConfigException("Neither [" + PROP_PREFIX + "zookeeper] nor [" + PROP_PREFIX
				+ "bootstrap.servers] found");
		zookeeperConnectionTimeoutMs = Long.valueOf(props.getProperty(PROP_PREFIX + "zookeeper.connection.timeout.ms", "20000"));
		transferBufferBytes = Long.valueOf(props.getProperty(PROP_PREFIX + "socket.buffer.bytes", Long.toString(5 * 1024 * 1024)));
		keySerializerClass = props.getProperty(PROP_PREFIX + "key.serializer.class", ByteArraySerializer.class.getName());
		valueSerializerClass = props.getProperty(PROP_PREFIX + "value.serializer.class", ByteArraySerializer.class.getName());
		poolSize = Long.parseLong(props.getProperty(PROP_PREFIX + "internal.pool.size", "3000000"));
		topics = Texts.split(props.getProperty(PROP_PREFIX + "topic", ""), ",");
		backoffMs = Long.parseLong(props.getProperty(PROP_PREFIX + "backoff.ms", "100"));
	}

	public KafkaConfigBase(URISpec uri) {
		super();
		String sch = uri.getScheme();
		if (sch.startsWith("kafka:")) sch = sch.substring(6);
		switch (sch) {
		case "zk":
		case "zk:kafka":
		case "zookeeper":
		case "kafka":
			String path = uri.getPath();
			String[] zks = new String[0];
			do {
				try (KafkaZkParser zk = new KafkaZkParser(uri.getHost() + path)) {
					zks = zk.getBrokers();
				} catch (Exception e) {
					logger.warn("ZK [" + uri.getHost() + path + "] detecting failure, try parent uri as ZK.", e);
				}
			} while (zks.length == 0 && !(path = path.replaceAll("/[^/]*$", "")).isEmpty());
			if (zks.length > 0) {
				bootstrapServers = String.join(",", zks);
				zookeeperConnect = uri.getHost() + path;
			} else {
				bootstrapServers = null;
				zookeeperConnect = uri.getHost() + uri.getPath();
			}
			break;
		case "bootstrap":
			bootstrapServers = uri.getHost();
			zookeeperConnect = null;
			break;
		default:
			throw new ConfigException("Neither [zk] nor [bootstrap.servers] found");
		}
		Map<String, String> props = uri.getParameters();
		zookeeperConnectionTimeoutMs = Long.valueOf(props.getOrDefault("zkconntimeout", "20000"));
		transferBufferBytes = Long.valueOf(props.getOrDefault("socketBuffer", Long.toString(5 * 1024 * 1024)));
		keySerializerClass = props.getOrDefault("kserial", ByteArraySerializer.class.getName());
		valueSerializerClass = props.getOrDefault("vserial", ByteArraySerializer.class.getName());
		poolSize = Long.parseLong(props.getOrDefault("pool", "3000000"));
		topics = new ArrayList<>(new HashSet<>(Texts.split(props.getOrDefault("topics", "") + "," + props.getOrDefault("topic", ""), ",")));
		backoffMs = Long.parseLong(props.getOrDefault("backoff", "100"));
	}

	public Properties props() {
		Properties props = new Properties();
		if (null != zookeeperConnect) props.setProperty("zookeeper.connect", zookeeperConnect);
		if (null != bootstrapServers) props.setProperty("bootstrap.servers", bootstrapServers);
		if (null != keySerializerClass) props.setProperty("key.serializer", keySerializerClass);
		if (null != valueSerializerClass) props.setProperty("value.serializer", valueSerializerClass);
		props.setProperty("connections.max.idle.ms", Long.toString(Long.MAX_VALUE));

		return props;
	}

	public String getZookeeperConnect() {
		return zookeeperConnect;
	}

	public List<String> topics() {
		return topics;
	}
}
