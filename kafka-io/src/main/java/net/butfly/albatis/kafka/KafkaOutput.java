package net.butfly.albatis.kafka;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;

import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.OutputImpl;
import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;

public final class KafkaOutput extends OutputImpl<KafkaMessage> {
	private final KafkaProducer<byte[], byte[]> connect;
	private final boolean async;

	public KafkaOutput(final String name, final String kafkaURI, boolean async) throws ConfigException {
		super(name);
		connect = new KafkaProducer<byte[], byte[]>(new KafkaOutputConfig(new URISpec(kafkaURI)).props());
		this.async = async;
		open();
	}

	public KafkaOutput(final String name, final String kafkaURI) throws ConfigException {
		this(name, kafkaURI, false);
	}

	@Override
	public void close() {
		super.close(connect::close);
	}

	@Override
	public boolean enqueue(KafkaMessage m) {
		if (null == m) return false;
		m.setTopic(m.getTopic());
		Future<RecordMetadata> r = connect.send(m.toProducer(), (meta, ex) -> {
			if (null != ex) logger().error("Kafka send failure on topic [" + m.getTopic() + "] with key: [" + new String(m.getKey()) + "]",
					ex);
		});
		try {
			r.get();
			return true;
		} catch (InterruptedException e) {
			return false;
		} catch (ExecutionException e) {
			logger().warn("Kafka send failure", e.getCause());
			return false;
		}
	}

	@Override
	public long enqueue(Stream<KafkaMessage> messages) {
		List<ListenableFuture<RecordMetadata>> fs = messages.filter(t -> t != null).map(msg -> JdkFutureAdapters.listenInPoolThread(connect
				.send(msg.toProducer()))).collect(Collectors.toList());
		if (!async) try {
			return Futures.successfulAsList(fs).get().parallelStream().filter(t -> t != null).count();
		} catch (InterruptedException e) {
			logger().error("[" + name() + "] interrupted", e);
		} catch (ExecutionException e) {
			logger().error("[" + name() + "] failure", e.getCause());
		}
		return fs.size();
	}

	public long fails() {
		return 0;
	}
}
