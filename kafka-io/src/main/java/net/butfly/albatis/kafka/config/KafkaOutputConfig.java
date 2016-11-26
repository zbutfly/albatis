package net.butfly.albatis.kafka.config;

import java.io.IOException;
import java.util.Properties;

import kafka.producer.ProducerConfig;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.utils.Configs;

public class KafkaOutputConfig extends KafkaConfigBase {
	private static final long serialVersionUID = -3028341800709486625L;
	private int retries;
	private int requestRequiredAcks;
	private String compressionCodec;

	public KafkaOutputConfig(String classpathResourceName) throws IOException {
		this(Configs.read(classpathResourceName));
	}

	public KafkaOutputConfig(Properties props) {
		super(props);

		requestRequiredAcks = Integer.parseInt(props.getProperty("albatis.kafka.request.required.acks", "-1"));
		compressionCodec = props.getProperty("albatis.kafka.compression.codec", "snappy");
		retries = Integer.parseInt(props.getProperty("albatis.kafka.retries", "0"));
	}

	public ProducerConfig getConfig() throws ConfigException {
		return new ProducerConfig(props());
	}

	@Override
	public Properties props() {
		Properties props = super.props();
		props.setProperty("metadata.fetch.timeout.ms", Long.toString(zookeeperConnectionTimeoutMs));
		props.setProperty("acks", Integer.toString(requestRequiredAcks));
		props.setProperty("compression.type", compressionCodec);
		props.setProperty("retries", Integer.toString(retries));
		props.setProperty("buffer.memory", Long.toString(transferBufferBytes));
		props.setProperty("send.buffer.bytes", Long.toString(transferBufferBytes));
		// props.setProperty("timeout.ms", "60000");
		// props.setProperty("metric.reporters", );
		// props.setProperty("metadata.max.age.ms", );
		// props.setProperty("batch.size", );
		// props.setProperty("reconnect.backoff.ms", );

		// props.setProperty("receive.buffer.bytes", );
		// props.setProperty("retry.backoff.ms", );

		// props.setProperty("max.request.size", );
		// props.setProperty("block.on.buffer.full", );
		// props.setProperty("metrics.sample.window.ms", );
		// props.setProperty("max.in.flight.requests.per.connection", );
		// props.setProperty("metrics.num.samples", );
		// props.setProperty("linger.ms", );
		// props.setProperty("client.id", );

		return props;
	}

	@Override
	public String toString() {
		return this.zookeeperConnect + "@" + this.bootstrapServers;
	}
}
