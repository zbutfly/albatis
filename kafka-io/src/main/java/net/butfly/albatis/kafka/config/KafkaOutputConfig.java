package net.butfly.albatis.kafka.config;

import java.util.Map;
import java.util.Properties;

import com.hzcominfo.albatis.nosql.Connection;

import kafka.producer.ProducerConfig;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.utils.URISpec;

public class KafkaOutputConfig extends KafkaConfigBase {
	private static final long serialVersionUID = -3028341800709486625L;
	final private boolean async;
	final private int retries;
	final private String requestRequiredAcks;
	final private String compressionCodec;
	final private int batchSize;
	final private boolean batchBlock;
	final private long bufferBytes;

	public KafkaOutputConfig(URISpec uri) {
		super(uri);
		Map<String, String> props = uri.getParameters();
		async = Boolean.parseBoolean(props.getOrDefault("async", "false"));
		requestRequiredAcks = props.getOrDefault("acks", "all");
		compressionCodec = props.getOrDefault("compression", "snappy");
		retries = Integer.parseInt(props.getOrDefault("retries", "0"));
		batchSize = Integer.parseInt(props.getOrDefault(Connection.PARAM_KEY_BATCH, "5000"));
		batchBlock = Boolean.parseBoolean(props.getOrDefault("block", "true"));
		bufferBytes = Long.parseLong(props.getOrDefault("buffer", Long.toString(128 * 1024 * 1024)));
	}

	/**
	 * @deprecated use {@link URISpec} to construct kafka configuration.
	 */
	@Deprecated
	public KafkaOutputConfig(Properties props) {
		super(props);
		async = Boolean.parseBoolean(props.getProperty(PROP_PREFIX + "async", "false"));
		requestRequiredAcks = props.getProperty(PROP_PREFIX + "request.required.acks", "all");
		compressionCodec = props.getProperty(PROP_PREFIX + "compression.codec", "snappy");
		retries = Integer.parseInt(props.getProperty(PROP_PREFIX + "retries", "0"));
		batchSize = Integer.parseInt(props.getProperty(PROP_PREFIX + "batch.size", "2000"));
		batchBlock = Boolean.parseBoolean(props.getProperty(PROP_PREFIX + "batch.block", "true"));
		bufferBytes = Long.parseLong(props.getProperty(PROP_PREFIX + "buffer", Long.toString(128 * 1024 * 1024)));
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

	@Override
	public Properties props() {
		Properties props = super.props();
		props.setProperty("max.block.ms", Long.toString(zookeeperConnectionTimeoutMs));
		props.setProperty("acks", requestRequiredAcks);
		props.setProperty("compression.type", compressionCodec);
		props.setProperty("retries", Integer.toString(retries));
		props.setProperty("buffer.memory", Long.toString(bufferBytes));
		props.setProperty("send.buffer.bytes", Long.toString(transferBufferBytes));
		// props.setProperty("timeout.ms", "60000");
		// props.setProperty("metric.reporters", );
		// props.setProperty("metadata.max.age.ms", );
		props.setProperty("batch.size", Long.toString(batchSize));
		props.setProperty("batch.block", Boolean.toString(batchBlock));
		props.setProperty("max.block.ms", Long.toString(Long.MAX_VALUE));
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

	public boolean isAsync() {
		return async;
	}

	@Override
	public String toString() {
		return this.zookeeperConnect + "@" + this.bootstrapServers;
	}
}
