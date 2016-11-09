package net.butfly.albatis.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.OutputQueue;
import net.butfly.albacore.io.OutputQueueImpl;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;

public class KafkaOutput<T> extends OutputQueueImpl<T, KafkaMessage> implements OutputQueue<T> {
	private static final long serialVersionUID = -8630366328993414430L;
	private final KafkaProducer<byte[], byte[]> connect;

	public KafkaOutput(final KafkaOutputConfig config, final Converter<T, KafkaMessage> conv) throws ConfigException {
		super("kafka-output-queue", conv);
		connect = new KafkaProducer<byte[], byte[]>(config.props());
	}

	@Override
	public void close() {
		connect.close();
	}

	@Override
	protected boolean enqueueRaw(T e) {
		KafkaMessage m = conv.apply(e);
		if (null == m) return false;
		connect.send(new ProducerRecord<byte[], byte[]>(m.getTopic(), m.getKey(), m.getBody()), (meta, ex) -> {
			if (null != ex) logger.error("Kafka send failure on topic [" + m.getTopic() + "] with key: [" + new String(m.getKey()) + "]",
					ex);
			stats(Act.INPUT, m);
		});
		return true;
	}
}
