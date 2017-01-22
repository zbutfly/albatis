package net.butfly.albatis.kafka;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.Output;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Collections;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaOutput extends Output<KafkaMessage> {
	private static final long serialVersionUID = -8630366328993414430L;
	private final KafkaProducer<byte[], byte[]> connect;

	public KafkaOutput(final String name, final String kafkaURI) throws ConfigException {
		super(name);
		connect = new KafkaProducer<byte[], byte[]>(new KafkaOutputConfig(new URISpec(kafkaURI)).props());
	}

	@Override
	public void close() {
		super.close();
		connect.close();
	}

	@Override
	public boolean enqueue0(KafkaMessage m) {
		if (null == m) return false;
		m.setTopic(m.getTopic());
		connect.send(m.toProducer(), (meta, ex) -> {
			if (null != ex) logger().error("Kafka send failure on topic [" + m.getTopic() + "] with key: [" + new String(m.getKey()) + "]",
					ex);
		});
		return true;
	}

	@Override
	public long enqueue(List<KafkaMessage> messages) {
		AtomicLong c = new AtomicLong(0);
		try {
			Futures.allAsList(Collections.transform(messages, msg -> JdkFutureAdapters.listenInPoolThread(connect.send(msg.toProducer(), (
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
