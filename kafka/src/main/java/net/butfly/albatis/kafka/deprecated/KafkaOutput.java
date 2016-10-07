package net.butfly.albatis.kafka.deprecated;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import net.butfly.albacore.io.OffHeapQueue;
import net.butfly.albacore.io.Output;
import net.butfly.albacore.io.Queue;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.kafka.KafkaException;
import net.butfly.albatis.kafka.Message;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;

@Deprecated
public class KafkaOutput implements Output<Message> {
	private static final long serialVersionUID = -276336973758504567L;
	private static final Logger logger = Logger.getLogger(KafkaOutput.class);

	private final Queue<byte[], byte[], byte[]> context;
	private final Producer<byte[], byte[]> connect;
	private AtomicBoolean closed;
	private OffHeapQueue<byte[], byte[]> q;

	/**
	 * @param mixed:
	 *            是否混合模式
	 * @param maxPackageMix:
	 *            混合模式下缓冲区个数
	 * @param maxMessageNoMix:
	 *            缓冲消息个数
	 * @return
	 */
	public KafkaOutput(final KafkaOutputConfig config) throws KafkaException {
		super();
		closed = new AtomicBoolean(false);
		String path = curr(config.getQueuePath(), config.toString());
		context = new OffHeapQueue<byte[], byte[]>("kafka-output", path, config.getPoolSize(), OffHeapQueue.C, OffHeapQueue.C);
		logger.trace("Writing threads pool created (max: " + 1 + ").");
		this.connect = connect(config);
	}

	@Override
	public void writing(Message... message) {
		byte[][] buf = new byte[message.length][];
		for (int i = 0; i < message.length; i++)
			buf[i] = Message.SERDER.ser(message[i]);
		context.enqueue(buf);
	}

	public void close() {
		closed.set(true);
		connect.close();
		context.close();
		q.close();
	}

	private String curr(String base, String folder) throws KafkaException {
		try {
			String path = base + "/" + Systems.getMainClass().getSimpleName() + "/" + folder.replaceAll("[:/\\,]", "-");
			File f = new File(path);
			f.mkdirs();
			return f.getCanonicalPath();
		} catch (IOException e) {
			throw new KafkaException(e);
		}
	}

	private Producer<byte[], byte[]> connect(KafkaOutputConfig config) {
		Producer<byte[], byte[]> p;
		try {
			p = new Producer<>(config.getConfig());
		} catch (KafkaException e) {
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
					Message m = Message.SERDER.der(b, Message.TOKEN);
					l.add(new KeyedMessage<byte[], byte[]>(m.getTopic(), m.getKey(), m.getBody()));
				}
				producer.send(l);
				logger.trace(() -> "Kafka service sent (amount: [" + msgs.size() + "]).");
			}
		}
	}
}
