package net.butfly.albatis.kafka.config;

import java.io.File;
import java.io.IOException;
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
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.kerberos.huawei.LoginUtil;
import net.butfly.albatis.tbds.TbdsAuthenticationUtil;

public abstract class KafkaConfigBase implements Serializable {
	private static final long serialVersionUID = -4020530608706621876L;
	public static final String PROP_PREFIX = "albatis.kafka.";

	// kerberos configs
	public static final String JAAS_CONF = "jaas.conf";
	public static final String KRB5_CONF = "krb5.conf";
	public static final String HUAWEI_KEYTAB = Configs.gets(PROP_PREFIX + "huawei.keytab", "user.keytab");
	public static final String KERBEROS_PROP_PATH = "kerberos.properties";
	public static Properties KERBEROS_PROPS = new Properties();
	public static boolean KRB5_CONF_EXIST = false;
	//tbds
	public static final String TBDS_PROP_PATH = "tbds.properties";
	
	private static final Logger logger = Logger.getLogger(KafkaConfigBase.class);

	protected final String zookeeperConnect;
	protected final String bootstrapServers;
	protected final Long backoffMs; // input: rebalance, output: retry
	protected final Long zookeeperConnectionTimeoutMs;
	protected final Long transferBufferBytes;
	protected final String metadataBrokerList;
	protected String keySerializerClass;
	protected String valueSerializerClass;
	protected String keyDeserializerClass;
	protected String valueDeserializerClass;
	protected final String version;
	protected String kerberosConfigPath;
	
	protected String tbdsConfigPath;

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
		metadataBrokerList = bootstrapServers;
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
		version = props.containsKey(PROP_PREFIX + "version") ? props.getProperty(PROP_PREFIX + "version") : null;
	}

	protected String bootstrapFromZk(String zookeeperConnect) {
		return null;
	}

	protected Pair<String, String> bootstrapFromZk(URISpec uri) { // XXX: not work?
		return new Pair<>(Configs.gets(PROP_PREFIX + "brokers"), uri.getHost() + uri.getPath());
	}

	public KafkaConfigBase(URISpec uri) {
		super();
		String[] schs = uri.getSchemas();
		String s = schs[0];
		if (schs.length > 1) s = schs[1];
		else switch (s) {
		case "kafka":// kafka < 0.9, default zk
			s = "zk";
			break;
		case "kafka2":// kafka 2, default bootstrap
			s = "bootstrap";
			break;
		}
		switch (s) {
		case "zk":
		case "zookeeper":
			logger.warn("Zookeeper Conenct mode is not supported by kafka now... \n"
					+ "\tUse bootstrap servers mode: kafka[2][:bootstrap]://host1[:port1][,host2[:port2][,...]]/");
			Pair<String, String> bAndZk = bootstrapFromZk(uri);
			bootstrapServers = bAndZk.v1();
			zookeeperConnect = bAndZk.v2();
			break;
		case "bootstrap": // directly bootstrap support kafka:bootstrap://172.30.1.3:2334/
			bootstrapServers = uri.getHost();
			zookeeperConnect = null;
			break;
		default:
			throw new ConfigException("Neither [zk] nor [bootstrap] found for uri [" + uri + "]");

		}
		kerberosConfigPath = uri.fetchParameter("kerberos");
		Map<String, String> props = uri.getParameters();
		zookeeperConnectionTimeoutMs = props.containsKey("zkconntimeout") ? Long.parseLong(props.get("zkconntimeout")) : null;
		transferBufferBytes = props.containsKey("socketBuffer") ? Long.parseLong(props.get("socketBuffer")) : null;
		poolSize = props.containsKey("pool") ? Long.parseLong(props.get("pool")) : null;
		backoffMs = props.containsKey("backoff") ? Long.parseLong(props.get("backoff")) : null;
		metadataBrokerList = bootstrapServers != null ? bootstrapServers : null;
		keySerializerClass = props.getOrDefault("kserial", ByteArraySerializer.class.getName());
		valueSerializerClass = props.getOrDefault("vserial", ByteArraySerializer.class.getName());
		keyDeserializerClass = props.getOrDefault("kdeserial", ByteArrayDeserializer.class.getName());
		valueDeserializerClass = props.getOrDefault("vdeserial", ByteArrayDeserializer.class.getName());
		version = props.get("version");
		topics = props.containsKey("topics") ? Colls.list(new HashSet<>(Texts.split(props.get("topics") + "," + props.get("topic"), ",")))
				: Colls.list();
		kerberosConfigPath = (null == kerberosConfigPath) ? Configs.gets(PROP_PREFIX + "kerberos") : kerberosConfigPath;
		tbdsConfigPath = Configs.gets(PROP_PREFIX + "tbds");
		kerberos();
	}

	public Properties props() {
		Properties props = new Properties();
		if (null != zookeeperConnect) props.setProperty("zookeeper.connect", zookeeperConnect);
		if (null != bootstrapServers) props.setProperty("bootstrap.servers", bootstrapServers);
		if (null != keySerializerClass) props.setProperty("key.serializer", keySerializerClass);
		if (null != valueSerializerClass) props.setProperty("value.serializer", valueSerializerClass);
		if (null != keyDeserializerClass) props.setProperty("key.deserializer", keyDeserializerClass);
		if (null != valueDeserializerClass) props.setProperty("value.deserializer", valueDeserializerClass);
		if (null != metadataBrokerList) props.setProperty("metadata.broker.list", metadataBrokerList);
		// props.setProperty("connections.max.idle.ms", Long.toString(Long.MAX_VALUE));
		if (null != version) props.setProperty("inter.broker.protocol.version", version);
		return props;
	}

	public String getZookeeperConnect() {
		return zookeeperConnect;
	}

	public List<String> topics() {
		return topics;
	}

	public void kerberos() {
		if (null == kerberosConfigPath) return;
		File kerberosConfigR = new File(kerberosConfigPath);
		String[] files = kerberosConfigR.list();
		List<String> fileList = Colls.list(files);
		try {
			KERBEROS_PROPS.load(IOs.openFile(kerberosConfigPath + KERBEROS_PROP_PATH));
		} catch (IOException e) {
			throw new RuntimeException("load KERBEROS_PROP error!", e);
			// logger.error("load KERBEROS_PROP error!", e);
		}
		if (null == KERBEROS_PROPS.getProperty("albatis.kafka.kerberos.jaas.enable") || !Boolean.parseBoolean(KERBEROS_PROPS.getProperty("albatis.kafka.kerberos.jaas.enable"))) return;
		if (fileList.contains(HUAWEI_KEYTAB)) {
			logger.info("Enable huawei kerberos!");
			try {
				LoginUtil.setJaasFile(KERBEROS_PROPS.getProperty("albatis.kafka.kerberos.kafka.principal"), kerberosConfigPath
						+ HUAWEI_KEYTAB);
				LoginUtil.setKrb5Config(kerberosConfigPath + KRB5_CONF);
				KRB5_CONF_EXIST = true;
				LoginUtil.setZookeeperServerPrincipal(KERBEROS_PROPS.getProperty("albatis.kafka.kerberos.zk.principal"));
			} catch (IOException e) {
				throw new RuntimeException("Load huawei kerberos config error!", e);
			}
		} else {
			File file = new File(kerberosConfigPath + KRB5_CONF);
			KRB5_CONF_EXIST = file.exists();
			if(!file.exists()){
				logger.info("Enable normal SASL/PLAIN");
				try {
					System.setProperty("java.security.auth.login.config", kerberosConfigPath + JAAS_CONF);
					logger.trace("java.security.auth.login.config:" +  kerberosConfigPath + JAAS_CONF);
				} catch (Exception e) {
					throw new RuntimeException("Load normal SASL/PLAIN config error!", e);
				}
			}else {
				logger.info("Enable normal kerberos!");
				try {
					LoginUtil.setKrb5Config(kerberosConfigPath + KRB5_CONF);
					if (null != KERBEROS_PROPS.getProperty("albatis.kafka.kerberos.zk.principal"))
						LoginUtil.setZookeeperServerPrincipal(KERBEROS_PROPS.getProperty("albatis.kafka.kerberos.zk.principal"));
					if (null != KERBEROS_PROPS.getProperty("albatis.kafka.kerberos.kafka.principal"))
						System.setProperty("albatis.kafka.kerberos.kafka.principal", KERBEROS_PROPS.getProperty("albatis.kafka.kerberos.kafka.principal"));
					System.setProperty("java.security.auth.login.config", kerberosConfigPath + JAAS_CONF);
				} catch (IOException e) {
					throw new RuntimeException("Load normal kerberos config error!", e);
				}
			}
		}
	}

	public void kerberosConfig(Properties props) {
		if (null != kerberosConfigPath) {
			if (null == KERBEROS_PROPS.getProperty("albatis.kafka.kerberos.jaas.enable") || !Boolean.parseBoolean(KERBEROS_PROPS.getProperty("albatis.kafka.kerberos.jaas.enable"))) {
				logger.info("Enable kerberos without jaas file path!");
				try {
					LoginUtil.setKrb5Config(kerberosConfigPath + KRB5_CONF);
				} catch (IOException e) {
					logger.error("load krb5.conf error!", e);
				}
				System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
				props.put("security.protocol",KERBEROS_PROPS.getProperty("security.protocol"));
				logger.trace("security.protocol" + KERBEROS_PROPS.getProperty("security.protocol"));
				props.put("sasl.mechanism",KERBEROS_PROPS.getProperty("sasl.mechanism"));
				logger.trace("sasl.mechanism" + KERBEROS_PROPS.getProperty("sasl.mechanism"));
				props.put("sasl.kerberos.service.name",KERBEROS_PROPS.getProperty("sasl.kerberos.service.name"));
				logger.trace("sasl.kerberos.service.name" + KERBEROS_PROPS.getProperty("sasl.kerberos.service.name"));
				props.put("sasl.jaas.config",KERBEROS_PROPS.getProperty("sasl.jaas.config"));
				logger.trace("sasl.jaas.config" + KERBEROS_PROPS.getProperty("sasl.jaas.config"));
				return;
			}
			if(!KRB5_CONF_EXIST){
				logger.info("Enable SASL/PLAIN! Config file path:" + kerberosConfigPath);
				props.put("security.protocol",KERBEROS_PROPS.getProperty("security.protocol"));
				logger.trace("security.protocol" + KERBEROS_PROPS.getProperty("security.protocol"));
				props.put("sasl.mechanism",KERBEROS_PROPS.getProperty("sasl.mechanism"));
				logger.trace("sasl.mechanism" + KERBEROS_PROPS.getProperty("sasl.mechanism"));
				if(null!=KERBEROS_PROPS.getProperty("schema.registry.url") && !"".equals(KERBEROS_PROPS.getProperty("schema.registry.url"))){
					props.put("schema.registry.url",KERBEROS_PROPS.getProperty("schema.registry.url"));
					logger.trace("schema.registry.url" + KERBEROS_PROPS.getProperty("schema.registry.url"));
				}
			}else {
				logger.info("Enable kerberos! Config file path:" + kerberosConfigPath);
				props.setProperty("kerberos.domain.name", KERBEROS_PROPS.getProperty("kerberos.domain.name"));
				logger.trace("kerberos.domain.name" + KERBEROS_PROPS.getProperty("kerberos.domain.name"));
				props.setProperty("security.protocol", KERBEROS_PROPS.getProperty("security.protocol"));
				logger.trace("security.protocol" + KERBEROS_PROPS.getProperty("security.protocol"));
				props.setProperty("sasl.kerberos.service.name", KERBEROS_PROPS.getProperty("sasl.kerberos.service.name"));
				logger.trace("sasl.kerberos.service.name" + KERBEROS_PROPS.getProperty("sasl.kerberos.service.name"));
			}
		}
	}
	
	public void tbds(Properties props) {
	    Properties TBDS_PROPS = new Properties();
		if (null == tbdsConfigPath) return;
		try {
			TBDS_PROPS.load(IOs.openFile(tbdsConfigPath + TBDS_PROP_PATH));
			props.put(TbdsAuthenticationUtil.KAFKA_SECURITY_PROTOCOL, TbdsAuthenticationUtil.KAFKA_SECURITY_PROTOCOL_AVLUE);
			props.put(TbdsAuthenticationUtil.KAFKA_SASL_MECHANISM, TbdsAuthenticationUtil.KAFKA_SASL_MECHANISM_VALUE);
			props.put(TbdsAuthenticationUtil.KAFKA_SASL_TBDS_SECURE_ID,TBDS_PROPS.getProperty("albatis.kafka.tbds.secureId"));
			props.put(TbdsAuthenticationUtil.KAFKA_SASL_TBDS_SECURE_KEY,TBDS_PROPS.getProperty("albatis.kafka.tbds.secureKey"));
		} catch (IOException e) {
			throw new RuntimeException("load TBDS_PROP error!", e);
		}
	}
}
