package net.butfly.albatis.kafka.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

public abstract class KafkaConfigBase implements Serializable {
	private static final long serialVersionUID = -4020530608706621876L;
	protected String zookeeperConnect;
	protected int zookeeperConnectionTimeoutMs;
	protected int transferBufferBytes;

	public KafkaConfigBase(Properties props) {
		super();
		zookeeperConnect = props.getProperty("albatis.kafka.zookeeper");
		zookeeperConnectionTimeoutMs = Integer.valueOf(props.getProperty("albatis.kafka.zookeeper.connection.timeout.ms", "15000"));
		transferBufferBytes = Integer.valueOf(props.getProperty("albatis.kafka.transfer.buffer.bytes", "5242880"));
	}

	public KafkaConfigBase(String classpathResourceName) {
		this(load(classpathResourceName));
	}

	protected Properties getProps() {
		Properties props = new Properties();
		props.setProperty("zookeeper.connect", zookeeperConnect);
		return props;
	}

	public String toString() {
		return zookeeperConnect;
	}

	private static Properties load(String classpathResourceName) {
		Properties props = new Properties();
		InputStream ips = Thread.currentThread().getContextClassLoader().getResourceAsStream(classpathResourceName);
		if (null == ips) throw new RuntimeException("Configuration not found: " + classpathResourceName);
		try {
			props.load(ips);
		} catch (IOException e) {
			throw new RuntimeException("Configuration could not load: " + classpathResourceName, e);
		}
		return props;
	}
}
