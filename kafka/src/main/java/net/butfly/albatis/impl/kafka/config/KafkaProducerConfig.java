package net.butfly.albatis.impl.kafka.config;

import java.util.Properties;

import kafka.producer.ProducerConfig;
import net.butfly.albatis.kafka.KafkaException;

@SuppressWarnings("deprecation")
public class KafkaProducerConfig extends KafkaConfigBase {
	private static final long serialVersionUID = -3028341800709486625L;
	public static final Properties DEFAULT_CONFIG = defaults();
	private String metadataBrokerList;
	private int requestRequiredAcks;
	private String producerType;
	private String compressionCodec;
	private String keySerializerClass;

	public KafkaProducerConfig(String zookeeperConnect, String metadataBrokerList) {
		this(DEFAULT_CONFIG);
		this.zookeeperConnect = zookeeperConnect;
		this.metadataBrokerList = metadataBrokerList;
	}

	public KafkaProducerConfig(String classpathResourceName) {
		super(classpathResourceName);
	}

	public KafkaProducerConfig(Properties props) {
		super(props);
		metadataBrokerList = props.getProperty("metadata.broker.list");
		requestRequiredAcks = Integer.parseInt(props.getProperty("request.required.acks", "-1"));
		producerType = props.getProperty("producer.type", "sync");
		compressionCodec = props.getProperty("compression.codec", "snappy");
		keySerializerClass = props.getProperty("key.serializer.class", "kafka.serializer.StringEncoder");
	}

	public ProducerConfig getConfig() throws KafkaException {
		if (zookeeperConnect == null || metadataBrokerList == null) throw new KafkaException(
				"Kafka configuration has no zookeeper and group definition.");
		return new ProducerConfig(getProps());
	}

	@Override
	protected Properties getProps() {
		Properties props = super.getProps();
		props.setProperty("zookeeper.connectiontimeout.ms", Integer.toString(zookeeperConnectionTimeoutMs));
		props.setProperty("send.buffer.bytes", Integer.toString(transferBufferBytes));
		props.setProperty("metadata.broker.list", metadataBrokerList);
		props.setProperty("request.required.acks", Integer.toString(requestRequiredAcks));
		props.setProperty("producer.type", producerType);
		props.setProperty("compression.codec", compressionCodec);
		props.setProperty("key.serializer.class", keySerializerClass);

		return props;
	}

	private static Properties defaults() {
		Properties props = new Properties();
		props.setProperty("zookeeper.connectiontimeout.ms", "15000");
		props.setProperty("send.buffer.bytes", "5120000");
		props.setProperty("request.required.acks", "-1");
		props.setProperty("producer.type", "sync");
		props.setProperty("compression.codec", "snappy");
		props.setProperty("key.serializer.class", "kafka.serializer.StringEncoder");
		return props;
	}
}
