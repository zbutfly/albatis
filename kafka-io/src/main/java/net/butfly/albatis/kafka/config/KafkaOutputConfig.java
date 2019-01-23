package net.butfly.albatis.kafka.config;

import java.util.Properties;

import kafka.producer.ProducerConfig;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Pair;

public class KafkaOutputConfig extends Kafka2OutputConfig {
	private static final long serialVersionUID = -1434502766892006389L;

	public KafkaOutputConfig(URISpec uri) {
		super(uri);
	}

	@Deprecated
	public KafkaOutputConfig(Properties props) {
		super(props);
	}

	public ProducerConfig getConfig() throws ConfigException {
		Properties props = props();
		Properties prod = new Properties();

		if (null != bootstrapServers) prod.setProperty("metadata.broker.list", bootstrapServers);
		if (props.containsKey("partitioner.class")) prod.setProperty("partitioner.class", props.getProperty("partitioner.class"));
		if (null != async) prod.setProperty("producer.type", async ? "async" : "sync");
		if (null != (compressionCodec)) prod.setProperty("compression.codec", compressionCodec);
		// prod.setProperty("compressed.topics", null);
		if (null != backoffMs) prod.setProperty("retry.backoff.ms", Long.toString(backoffMs));
		if (null != retries) prod.setProperty("message.send.max.retries", Integer.toString(retries));
		// prod.setProperty("topic.metadata.refresh.interval.ms", 600000);
		return new ProducerConfig(prod);
	}

	@Override
	protected Pair<String, String> bootstrapFromZk(URISpec uri) {
		return KafkaZkParser.bootstrapFromZk(uri);
	}
}
