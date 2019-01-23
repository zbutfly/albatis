package net.butfly.albatis.kafka.config;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Logger;

public abstract class KafkaConfigBase implements Serializable {
	private static final long serialVersionUID = -4020530608706621876L;
	public static final String PROP_PREFIX = "albatis.kafka.";
	private static final Logger logger = Logger.getLogger(KafkaConfigBase.class);

	protected final String zookeeperConnect;
	protected final String bootstrapServers;
	protected final Long backoffMs; // input: rebalance, output: retry
	protected final Long zookeeperConnectionTimeoutMs;
	protected final Long transferBufferBytes;
	protected String keySerializerClass;
	protected String valueSerializerClass;
	protected String keyDeserializerClass;
	protected String valueDeserializerClass;

	private final Long poolSize;
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
		String bstrp = props.getProperty(PROP_PREFIX + "bootstrap.servers");
		if (zookeeperConnect != null) {
			if (bstrp != null) logger.warn("Zookeeper detect broken list automatically, configured [" + PROP_PREFIX
					+ "bootstrap.servers] is not used (current value: [" + bstrp + "])");
			bstrp = bootstrapFromZk(zookeeperConnect);
		} else if (bstrp == null) throw new ConfigException("Neither [" + PROP_PREFIX + "zookeeper] nor [" + PROP_PREFIX
				+ "bootstrap.servers] found");
		bootstrapServers = bstrp;
		zookeeperConnectionTimeoutMs = props.containsKey(PROP_PREFIX + "zookeeper.connection.timeout.ms") ? //
				Long.parseLong(props.getProperty(PROP_PREFIX + "zookeeper.connection.timeout.ms")) : null;
		transferBufferBytes = props.containsKey(PROP_PREFIX + "socket.buffer.bytes") ? //
				Long.parseLong(props.getProperty(PROP_PREFIX + "socket.buffer.bytes")) : null;
		keySerializerClass = props.getProperty(PROP_PREFIX + "key.serializer.class");
		valueSerializerClass = props.getProperty(PROP_PREFIX + "value.serializer.class");
		poolSize = props.containsKey(PROP_PREFIX + "internal.pool.size") ? //
				Long.parseLong(props.getProperty(PROP_PREFIX + "internal.pool.size")) : null;
		topics = Texts.split(props.getProperty(PROP_PREFIX + "topic", ""), ",");
		backoffMs = props.containsKey(PROP_PREFIX + "backoff.ms") ? //
				Long.parseLong(props.getProperty(PROP_PREFIX + "backoff.ms")) : null;
	}

	protected String bootstrapFromZk(String zookeeperConnect) {
		return null;
	}

	protected Pair<String, String> bootstrapFromZk(URISpec uri) { // XXX: not work?
		return new Pair<>(Configs.gets(PROP_PREFIX + "brokers"), uri.getHost() + uri.getPath());
	}

	public KafkaConfigBase(URISpec uri) {
		super();
		String sch = uri.getScheme();
		if (sch.startsWith("kafka:")) sch = sch.substring(6);
		else if (sch.startsWith("kafka2:")) sch = sch.substring(7);
		switch (sch) {
		case "kafka":
		case "kafka2": // empty sub schema, default as zk
		case "zk":
		case "zookeeper":
			Pair<String, String> bAndZk = bootstrapFromZk(uri);
			bootstrapServers = bAndZk.v1();
			zookeeperConnect = bAndZk.v2();
			break;
		case "bootstrap": // directly bootstrap support
			bootstrapServers = uri.getHost();
			zookeeperConnect = null;
			break;
		default:
			throw new ConfigException("Neither [zk] nor [bootstrap] found for uri [" + uri + "]");
		}
		Map<String, String> props = uri.getParameters();
		zookeeperConnectionTimeoutMs = props.containsKey("zkconntimeout") ? Long.parseLong(props.get("zkconntimeout")) : null;
		transferBufferBytes = props.containsKey("socketBuffer") ? Long.parseLong(props.get("socketBuffer")) : null;
		poolSize = props.containsKey("pool") ? Long.parseLong(props.get("pool")) : null;
		backoffMs = props.containsKey("backoff") ? Long.parseLong(props.get("backoff")) : null;

		keySerializerClass = props.getOrDefault("kserial", ByteArraySerializer.class.getName());
		valueSerializerClass = props.getOrDefault("vserial", ByteArraySerializer.class.getName());
		keyDeserializerClass = props.getOrDefault("kdeserial", ByteArrayDeserializer.class.getName());
		valueDeserializerClass = props.getOrDefault("vdeserial", ByteArrayDeserializer.class.getName());

		topics = props.containsKey("topics") ? Colls.list(new HashSet<>(Texts.split(props.get("topics") + "," + props.get("topic"), ",")))
				: Colls.list();
	}

	public Properties props() {
		Properties props = new Properties();
		if (null != zookeeperConnect) props.setProperty("zookeeper.connect", zookeeperConnect);
		if (null != bootstrapServers) props.setProperty("bootstrap.servers", bootstrapServers);
		if (null != keySerializerClass) props.setProperty("key.serializer", keySerializerClass);
		if (null != valueSerializerClass) props.setProperty("value.serializer", valueSerializerClass);
		if (null != keyDeserializerClass) props.setProperty("key.deserializer", keyDeserializerClass);
		if (null != valueDeserializerClass) props.setProperty("value.deserializer", valueDeserializerClass);
		// props.setProperty("connections.max.idle.ms", Long.toString(Long.MAX_VALUE));

		return props;
	}

	public String getZookeeperConnect() {
		return zookeeperConnect;
	}

	public List<String> topics() {
		return topics;
	}
}
