package net.butfly.albatis.kafka;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;

import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.OutputImpl;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Collections;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;

public final class KafkaOutput extends OutputImpl<KafkaMessage> {
	private final KafkaProducer<byte[], byte[]> connect;

	public KafkaOutput(final String name, final String kafkaURI) throws ConfigException {
		super(name);
		connect = new KafkaProducer<byte[], byte[]>(new KafkaOutputConfig(new URISpec(kafkaURI)).props());
		open();
	}

	@Override
	public void close() {
		super.close(connect::close);
	}

	@Override
	public boolean enqueue(KafkaMessage m, boolean block) {
		if (null == m) return false;
		m.setTopic(m.getTopic());
		Future<RecordMetadata> r = connect.send(m.toProducer(), (meta, ex) -> {
			if (null != ex) logger().error("Kafka send failure on topic [" + m.getTopic() + "] with key: [" + new String(m.getKey()) + "]",
					ex);
		});
		if (block) try {
			r.get();
			return true;
		} catch (InterruptedException | ExecutionException e) {
			return false;
		}
		else return true;
	}

	@Override
	public long enqueue(List<KafkaMessage> messages) {
		AtomicLong c = new AtomicLong(0);
		try {
			Futures.allAsList(Collections.mapNoNullIn(messages, msg -> JdkFutureAdapters.listenInPoolThread(connect.send(msg.toProducer(), (
					meta, ex) -> {
				if (null != ex) logger().error("Kafka send failure on topic [" + msg.getTopic() + "] with key: [" + new String(msg.getKey())
						+ "]", ex);
				else c.incrementAndGet();// TODO: Failover
			})))).get();
			logger().trace("[" + name() + "] sent: " + messages.size());
		} catch (InterruptedException e) {
			logger().error("[" + name() + "] interrupted", e);
		} catch (ExecutionException e) {
			logger().error("[" + name() + "] failure", e.getCause());
		}
		return c.get();
	}

	public long fails() {
		return 0;
	}
}
