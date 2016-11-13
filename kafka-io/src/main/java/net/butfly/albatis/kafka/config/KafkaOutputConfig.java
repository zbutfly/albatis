package net.butfly.albatis.kafka.config;

import java.io.IOException;
import java.util.Properties;

import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.utils.Configs;

public class KafkaOutputConfig extends KafkaConfigBase {
	private static final long serialVersionUID = -3028341800709486625L;
	private String metadataBrokerList;
	private int requestRequiredAcks;
	private String producerType;
	private String compressionCodec;
	private String keySerializerClass;

	public KafkaOutputConfig(String classpathResourceName) throws IOException {
		this(Configs.read(classpathResourceName));
	}

	public KafkaOutputConfig(Properties props) {
		super(props);
		metadataBrokerList = props.getProperty("albatis.kafka.metadata.broker.list");

		requestRequiredAcks = Integer.parseInt(props.getProperty("albatis.kafka.request.required.acks", "-1"));
		producerType = props.getProperty("albatis.kafka.producer.type", "sync");
		compressionCodec = props.getProperty("albatis.kafka.compression.codec", "snappy");
		keySerializerClass = props.getProperty("albatis.kafka.key.serializer.class", StringEncoder.class.getName());
	}

	public ProducerConfig getConfig() throws ConfigException {
		if (zookeeperConnect == null || metadataBrokerList == null) throw new ConfigException(
				"Kafka configuration has no zookeeper and group definition.");
		return new ProducerConfig(props());
	}

	@Override
	public Properties props() {
		Properties props = super.props();
		props.setProperty("zookeeper.connectiontimeout.ms", Long.toString(zookeeperConnectionTimeoutMs));
		props.setProperty("send.buffer.bytes", Long.toString(transferBufferBytes));
		props.setProperty("metadata.broker.list", metadataBrokerList);
		props.setProperty("request.required.acks", Integer.toString(requestRequiredAcks));
		props.setProperty("producer.type", producerType);
		props.setProperty("compression.codec", compressionCodec);
		props.setProperty("key.serializer.class", keySerializerClass);

		return props;
	}

	@Override
	public String toString() {
		return this.zookeeperConnect + "@" + this.metadataBrokerList;
	}
}
