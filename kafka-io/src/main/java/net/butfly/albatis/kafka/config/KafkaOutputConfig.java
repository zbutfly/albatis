package net.butfly.albatis.kafka.config;

import java.util.Properties;

import kafka.producer.ProducerConfig;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;

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

		prod.setProperty("metadata.broker.list", bootstrapServers);
		prod.setProperty("partitioner.class", props.getProperty("partitioner.class", "kafka.producer.DefaultPartitioner"));
		prod.setProperty("producer.type", async ? "async" : "sync");
		prod.setProperty("compression.codec", compressionCodec);
		// prod.setProperty("compressed.topics", null);
		prod.setProperty("retry.backoff.ms", Long.toString(backoffMs));
		prod.setProperty("message.send.max.retries", Integer.toString(retries));
		// prod.setProperty("topic.metadata.refresh.interval.ms", 600000);
		return new ProducerConfig(prod);
	}
}
