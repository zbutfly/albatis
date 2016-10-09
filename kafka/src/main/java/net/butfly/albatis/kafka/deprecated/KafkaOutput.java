package net.butfly.albatis.kafka.deprecated;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.OffHeapQueueImpl;
import net.butfly.albacore.io.Queue;
import net.butfly.albacore.io.SimpleOffHeapQueue;
import net.butfly.albacore.io.deprecated.Output;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.kafka.KafkaMessage;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;

@Deprecated
public class KafkaOutput implements Output<KafkaMessage> {
	private static final long serialVersionUID = -276336973758504567L;
	private static final Logger logger = Logger.getLogger(KafkaOutput.class);

	private final Queue<byte[], byte[]> context;
	private final Producer<byte[], byte[]> connect;
	private AtomicBoolean closed;
	private OffHeapQueueImpl<byte[], byte[]> q;

	/**
	 * @param queuePath
	 * @param mixed:
	 *            是否混合模式
	 * @param maxPackageMix:
	 *            混合模式下缓冲区个数
	 * @param maxMessageNoMix:
	 *            缓冲消息个数
	 * @return
	 */
	public KafkaOutput(final KafkaOutputConfig config, String queuePath) throws ConfigException {
		super();
		closed = new AtomicBoolean(false);
		String path = Local.curr(queuePath, config.toString());
		context = new SimpleOffHeapQueue("kafka-output", path, config.getPoolSize());
		logger.trace("Writing threads pool created (max: " + 1 + ").");
		this.connect = connect(config);
	}

	@Override
	public void writing(KafkaMessage... message) {
		byte[][] buf = new byte[message.length][];
		for (int i = 0; i < message.length; i++)
			buf[i] = message[i].toBytes();
		context.enqueue(buf);
	}

	public void close() {
		closed.set(true);
		connect.close();
		context.close();
		q.close();
	}

	private Producer<byte[], byte[]> connect(KafkaOutputConfig config) {
		Producer<byte[], byte[]> p;
		try {
			p = new Producer<>(config.getConfig());
		} catch (ConfigException e) {
			throw new RuntimeException(e);
		}
		Concurrents.submit(new OutputThread(p, config.getBatchSize()));
		return p;
	}

	class OutputThread extends Thread {
		private Producer<byte[], byte[]> producer;
		private long batchSize;

		OutputThread(Producer<byte[], byte[]> p, long batchSize) {
			super();
			this.producer = p;
			this.batchSize = batchSize;
		}

		@Override
		public void run() {
			while (!closed.get()) {
				List<byte[]> msgs = context.dequeue(batchSize);
				List<KeyedMessage<byte[], byte[]>> l = new ArrayList<>(msgs.size());
				for (byte[] b : msgs) {
					KafkaMessage m = new KafkaMessage(b);
					l.add(new KeyedMessage<byte[], byte[]>(m.getTopic(), m.getKey(), m.getBody()));
				}
				producer.send(l);
				logger.trace(() -> "Kafka service sent (amount: [" + msgs.size() + "]).");
			}
		}
	}
}
