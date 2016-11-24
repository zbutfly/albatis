package net.butfly.albatis.kafka;

import java.util.ArrayList;
import java.util.Iterator;
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
import net.butfly.albacore.io.MapOutputImpl;
import net.butfly.albacore.io.queue.Q;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;

public class KafkaOutput extends MapOutputImpl<String, KafkaMessage> {
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
	public long enqueue(String topic, Iterator<KafkaMessage> messages) {
		if (null == messages) return 0;
		List<ListenableFuture<RecordMetadata>> fs = new ArrayList<>();
		AtomicLong c = new AtomicLong(0);
		while (messages.hasNext()) {
			KafkaMessage m = messages.next();
			fs.add(JdkFutureAdapters.listenInPoolThread(connect.send(m.toProducer(), (meta, ex) -> {
				if (null != ex) logger.error("Kafka send failure on topic [" + topic + "] with key: [" + new String(m.getKey()) + "]", ex);
				else c.incrementAndGet();
			})));
		}
		try {
			Futures.successfulAsList(fs).get();
		} catch (InterruptedException e) {
			logger.error("KafkaOutput interrupted", e);
		} catch (ExecutionException e) {
			logger.error("KafkaOutput failure", e.getCause());
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
