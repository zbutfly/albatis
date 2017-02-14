package net.butfly.albatis.kafka;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;

import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.OutputImpl;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Exceptions;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;

/**
 * @author zx // * @deprecated memory leak!
 */
public final class KafkaOutput extends OutputImpl<KafkaMessage> {
	private final URISpec uri;
	private final KafkaOutputConfig config;
	private final KafkaProducer<byte[], byte[]> producer;

	public KafkaOutput(final String name, final String kafkaURI, boolean async) throws ConfigException {
		super(name);
		uri = new URISpec(kafkaURI);
		config = new KafkaOutputConfig(uri);
		producer = new KafkaProducer<byte[], byte[]>(config.props());
		open();
	}

	public KafkaOutput(final String name, final String kafkaURI) throws ConfigException {
		this(name, kafkaURI, false);
	}

	@Override
	public void close() {
		super.close(producer::close);
	}

	@Override
	public boolean enqueue(KafkaMessage m) {
		if (null == m) return false;
		Future<RecordMetadata> future = send(m);
		if (null == future) return false;
		if (config.isAsync()) return true;
		try {
			future.get();
			return true;
		} catch (InterruptedException e) {
			return false;
		} catch (Exception e) {
			logger().warn("Kafka send failure", Exceptions.unwrap(e));
			return false;
		}
	}

	@Override
	public long enqueue(Stream<KafkaMessage> messages) {
		List<ListenableFuture<RecordMetadata>> futures = io.list(messages.map(msg -> JdkFutureAdapters.listenInPoolThread(send(msg))));
		if (config.isAsync()) return futures.size();
		try {
			return io.collect(Futures.successfulAsList(futures).get(), Collectors.counting());
		} catch (InterruptedException e) {
			logger().error("[" + name() + "] interrupted", e);
			return 0;
		} catch (ExecutionException e) {
			logger().error("[" + name() + "] failure", Exceptions.unwrap(e));
			return 0;
		}
	}

	private Future<RecordMetadata> send(KafkaMessage msg) {
		Future<RecordMetadata> future = null;
		boolean retry = true;
		do {
			try {
				future = producer.send(msg.toProducer());
				retry = false;
			} catch (BufferExhaustedException e) {
				logger().warn("Kafka producer buffer exhausted, retry until success");
				if (!Concurrents.waitSleep()) return null;
			}
		} while (retry);
		return JdkFutureAdapters.listenInPoolThread(future);
	}

	public long fails() {
		return 0;
	}

	public String getDefaultTopic() {
		String[] topics = config.getTopics();
		return topics == null || topics.length == 0 ? null : topics[0];
	}
}
