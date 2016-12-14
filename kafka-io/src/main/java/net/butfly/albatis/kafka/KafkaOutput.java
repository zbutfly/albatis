package net.butfly.albatis.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;

import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.MapOutput;
import net.butfly.albacore.io.queue.Q;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;

public class KafkaOutput extends MapOutput<String, KafkaMessage> {
	private static final long serialVersionUID = -8630366328993414430L;
	private final KafkaProducer<byte[], byte[]> connect;

	public KafkaOutput(final String name, final KafkaOutputConfig config) throws ConfigException {
		super(name, km -> km.getTopic());
		connect = new KafkaProducer<byte[], byte[]>(config.props());
	}

	@Override
	public void close() {
		connect.close();
	}

	@Override
	public boolean enqueue0(String topic, KafkaMessage m) {
		if (null == m) return false;
		m.setTopic(topic);
		connect.send(m.toProducer(), (meta, ex) -> {
			if (null != ex) logger.error("Kafka send failure on topic [" + m.getTopic() + "] with key: [" + new String(m.getKey()) + "]",
					ex);
		});
		return true;
	}

	@Override
	public long enqueue(Converter<KafkaMessage, String> topicing, List<KafkaMessage> messages) {
		List<ListenableFuture<RecordMetadata>> fs = new ArrayList<>();
		AtomicLong c = new AtomicLong(0);
		for (KafkaMessage m : messages) {
			m.setTopic(topicing.apply(m));
			fs.add(JdkFutureAdapters.listenInPoolThread(connect.send(m.toProducer(), (meta, ex) -> {
				if (null != ex) logger.error("Kafka send failure on topic [" + m.getTopic() + "] with key: [" + new String(m.getKey())
						+ "]", ex);
				else c.incrementAndGet();
			})));
		}
		try {
			Futures.successfulAsList(fs).get();
		} catch (InterruptedException e) {
			logger.error("KafkaOutput [" + name() + "] interrupted", e);
		} catch (ExecutionException e) {
			logger.error("KafkaOutput [" + name() + "] failure", e.getCause());
		}
		return c.get();
	}

	@Override
	public Set<String> keys() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Q<KafkaMessage, Void> q(String key) {
		throw new UnsupportedOperationException();
	}
}
