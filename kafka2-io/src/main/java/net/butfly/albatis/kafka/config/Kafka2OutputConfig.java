package net.butfly.albatis.kafka.config;

import java.util.Map;
import java.util.Properties;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.Connection;

public class Kafka2OutputConfig extends KafkaConfigBase {
	private static final long serialVersionUID = -3028341800709486625L;
	public final Boolean async;
	public final Integer retries;
	public final String requestRequiredAcks;
	public final String compressionCodec;
	public final Integer batchSize;
	public final Boolean batchBlock;
	public final Long bufferBytes;

	public Kafka2OutputConfig(URISpec uri) {
		super(uri);
		Map<String, String> props = uri.getParameters();
		async = props.containsKey("async") ? Boolean.parseBoolean(props.get("async")) : null;
		requestRequiredAcks = props.get("acks");
		compressionCodec = props.get("compression");
		retries = props.containsKey("retries") ? Integer.parseInt(props.get("retries")) : null;
		batchSize = props.containsKey(Connection.PARAM_KEY_BATCH) ? Integer.parseInt(props.get(Connection.PARAM_KEY_BATCH)) : null;
		batchBlock = props.containsKey("block") ? Boolean.parseBoolean(props.get("block")) : null;
		bufferBytes = props.containsKey("buffer") ? Long.parseLong(props.get("buffer")) : null;
	}

	/**
	 * @deprecated use {@link URISpec} to construct kafka configuration.
	 */
	@Deprecated
	public Kafka2OutputConfig(Properties props) {
		super(props);
		async = props.containsKey(PROP_PREFIX + "async") ? Boolean.parseBoolean(props.getProperty(PROP_PREFIX + "async")) : null;
		requestRequiredAcks = props.containsKey(PROP_PREFIX + "request.required.acks") ? //
				props.getProperty(PROP_PREFIX + "request.required.acks") : null;
		compressionCodec = props.containsKey(PROP_PREFIX + "compression.codec") ? //
				props.getProperty(PROP_PREFIX + "compression.codec") : null;
		retries = props.containsKey(PROP_PREFIX + "retries") ? Integer.parseInt(props.getProperty(PROP_PREFIX + "retries")) : null;
		batchSize = props.containsKey(PROP_PREFIX + "batch.size") ? Integer.parseInt(props.getProperty(PROP_PREFIX + "batch.size")) : null;
		batchBlock = props.containsKey(PROP_PREFIX + "batch.block") ? //
				Boolean.parseBoolean(props.getProperty(PROP_PREFIX + "batch.block")) : null;
		bufferBytes = props.containsKey(PROP_PREFIX + "buffer") ? Long.parseLong(props.getProperty(PROP_PREFIX + "buffer")) : null;
	}

	@Override
	public Properties props() {
		Properties props = super.props();
		if (null != zookeeperConnectionTimeoutMs) props.setProperty("max.block.ms", Long.toString(zookeeperConnectionTimeoutMs));
		if (null != requestRequiredAcks) props.setProperty("acks", requestRequiredAcks);
		if (null != compressionCodec) props.setProperty("compression.type", compressionCodec);
		if (null != retries) props.setProperty("retries", Integer.toString(retries));
		if (null != bufferBytes) props.setProperty("buffer.memory", Long.toString(bufferBytes));
		if (null != transferBufferBytes) props.setProperty("send.buffer.bytes", Long.toString(transferBufferBytes));
		// props.setProperty("timeout.ms");
		// props.setProperty("metric.reporters", );
		// props.setProperty("metadata.max.age.ms", );
		if (null != batchSize) props.setProperty("batch.size", Long.toString(batchSize));
		if (null != batchBlock) props.setProperty("batch.block", Boolean.toString(batchBlock));

//		props.setProperty("receive.buffer.bytes", Long.toString(backoffMs));
		if (null != backoffMs) {
			props.setProperty("retry.backoff.ms", Long.toString(backoffMs));
			props.setProperty("reconnect.backoff.ms", Long.toString(backoffMs));
		}
		// props.setProperty("max.request.size", );
		// props.setProperty("block.on.buffer.full", );
		// props.setProperty("metrics.sample.window.ms", );
		// props.setProperty("max.in.flight.requests.per.connection", );
		// props.setProperty("metrics.num.samples", );
		// props.setProperty("linger.ms", );
		// props.setProperty("client.id", );
		kerberosConfig(props);
		return props;
	}

	public boolean isAsync() {
		return null != async && async.booleanValue();
	}

	@Override
	public String toString() {
		return this.zookeeperConnect + "@" + this.bootstrapServers;
	}
}
