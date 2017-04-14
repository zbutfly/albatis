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

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.Output;
import net.butfly.albacore.io.utils.Parals;
import net.butfly.albacore.io.utils.URISpec;
import net.butfly.albacore.utils.Exceptions;
import net.butfly.albacore.utils.parallel.Concurrents;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;

/**
 * @author zx
 * @deprecated memory leak!
 */
@Deprecated
public final class KafkaOutput0 extends Namedly implements Output<KafkaMessage> {
	private final URISpec uri;
	private final KafkaOutputConfig config;
	private final KafkaProducer<byte[], byte[]> producer;

	public KafkaOutput0(final String name, final String kafkaURI, boolean async) throws ConfigException {
		super(name);
		uri = new URISpec(kafkaURI);
		config = new KafkaOutputConfig(uri);
		producer = new KafkaProducer<byte[], byte[]>(config.props());
		closing(producer::close);
		open();
	}

	public KafkaOutput0(final String name, final String kafkaURI) throws ConfigException {
		this(name, kafkaURI, false);
	}

	@Override
	public long enqueue(Stream<KafkaMessage> messages) {
		List<ListenableFuture<RecordMetadata>> futures = Parals.list(messages.map(msg -> JdkFutureAdapters.listenInPoolThread(send(msg))));
		if (config.isAsync()) return futures.size();
		try {
			return Parals.collect(Futures.successfulAsList(futures).get(), Collectors.counting());
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
		return config.topics().isEmpty() ? null : config.topics().get(0);
	}
}
