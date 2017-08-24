package net.butfly.albatis.kafka.config;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import com.google.common.base.Joiner;

import net.butfly.albacore.io.utils.URISpec;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.kafka.ZKConn;

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
			try (ZKConn zk = new ZKConn(zookeeperConnect)) {
				bootstrapServers = Joiner.on(",").join(zk.getBorkers());
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
		case "zookeeper":
		case "kafka":
			String u = uri.getHost() + uri.getPathOnly();
			if (u.endsWith("/")) u = u.substring(0, u.length() - 1);
			try (ZKConn zk = new ZKConn(u)) {
				bootstrapServers = Joiner.on(",").join(zk.getBorkers());
				logger.trace("Zookeeper detect broken list automatically: [" + bootstrapServers + "])");
			} catch (Exception e) {
				u = uri.getHost() + uri.getPath();
				try (ZKConn zk = new ZKConn(u)) {
					bootstrapServers = Joiner.on(",").join(zk.getBorkers());
					logger.trace("Zookeeper detect broken list automatically: [" + bootstrapServers + "])");
				} catch (Exception ee) {
					bootstrapServers = null;
					logger.warn("Zookeeper detect broken list failure", ee);
				}
			}
			zookeeperConnect = u;
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
		Set<String> ts = new HashSet<>(Texts.split(uri.getFile(), ","));
		ts.addAll(Texts.split(props.getOrDefault("topic", ""), ","));
		topics = new ArrayList<>(ts);
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
