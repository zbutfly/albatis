package net.butfly.albatis.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.OutputQueue;
import net.butfly.albacore.io.OutputQueueImpl;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;

public class KafkaOutputQueue extends OutputQueueImpl<KafkaMessage, KafkaMessage> implements OutputQueue<KafkaMessage> {
	private static final long serialVersionUID = -8630366328993414430L;
	private final KafkaProducer<byte[], byte[]> connect;

	public KafkaOutputQueue(final KafkaOutputConfig config) throws ConfigException {
		super("kafka-output-queue", Long.MAX_VALUE);
		connect = new KafkaProducer<byte[], byte[]>(config.props());
	}

	public void close() {
		connect.close();
	}

	@Override
	public long size() {
		return 0;
	}

	@Override
	protected boolean enqueueRaw(KafkaMessage e) {
		connect.send(new ProducerRecord<byte[], byte[]>(e.getTopic(), e.getKey(), e.getBody()), (meta, ex) -> {
			if (null != ex) logger.error("Kafka send failure on topic [" + e.getTopic() + "] with key: [" + new String(e.getKey()) + "]",
					ex);
			stats(Act.INPUT, e);
		});
		return true;
	}
}
