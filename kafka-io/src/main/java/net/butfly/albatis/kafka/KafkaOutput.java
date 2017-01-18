package net.butfly.albatis.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;

import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.Output;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Collections;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;

public class KafkaOutput extends Output<KafkaMessage> {
	private static final long serialVersionUID = -8630366328993414430L;
	private final KafkaProducer<byte[], byte[]> connect;

	public KafkaOutput(final String name, final String kafkaURI) throws ConfigException {
		super(name);
		connect = new KafkaProducer<byte[], byte[]>(new KafkaOutputConfig(new URISpec(kafkaURI)).props());
	}

	@Override
	public void closing() {
		super.closing();
		connect.close();
	}

	@Override
	public boolean enqueue0(KafkaMessage m) {
		if (null == m) return false;
		m.setTopic(m.getTopic());
		connect.send(m.toProducer(), (meta, ex) -> {
			if (null != ex) logger.error("Kafka send failure on topic [" + m.getTopic() + "] with key: [" + new String(m.getKey()) + "]",
					ex);
		});
		return true;
	}

	@Override
	public long enqueue(List<KafkaMessage> messages) {
		AtomicLong c = new AtomicLong(0);
		Collections.transform(messages, m -> JdkFutureAdapters.listenInPoolThread(connect.send(m.toProducer(), (meta, ex) -> {
			if (null != ex) logger.error("Kafka send failure on topic [" + m.getTopic() + "] with key: [" + new String(m.getKey()) + "]",
					ex);
			else c.incrementAndGet();// TODO: Failover
		})));
		// try {
		// List<RecordMetadata> results = Futures.successfulAsList(fs).get();
		// logger.trace("[" + name() + "] sent: " + results.size());
		// } catch (InterruptedException e) {
		// logger.error("[" + name() + "] interrupted", e);
		// } catch (ExecutionException e) {
		// logger.error("[" + name() + "] failure", e.getCause());
		// }
		return messages.size();
	}

	public long fails() {
		return 0;
	}
}
