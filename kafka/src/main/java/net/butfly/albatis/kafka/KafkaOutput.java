package net.butfly.albatis.kafka;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import net.butfly.albacore.io.Output;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.serder.BsonSerder;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.kafka.Queue.Message;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;

public class KafkaOutput implements Output<Message> {
	private static final long serialVersionUID = -276336973758504567L;
	private static final Logger logger = Logger.getLogger(KafkaOutput.class);

	private final Queue context;
	private final BsonSerder serder;
	private final Producer<byte[], byte[]> connect;
	private AtomicBoolean closed;

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
		this.serder = new BsonSerder();
		context = new Queue(curr(config.getQueuePath(), config.toString()), config.getPoolSize());
		logger.trace("Writing threads pool created (max: " + 1 + ").");
		this.connect = connect(config);
	}

	@Override
	public void writing(Message... message) {
		context.enqueue(message);
	}

	@SafeVarargs
	public final void write(String topic, Converter<Map<String, Object>, byte[]> keying, Map<String, Object>... object) {
		Message[] messages = new Message[object.length];
		for (int i = 0; i < object.length; i++)
			messages[i] = new Message(topic, keying.apply(object[i]), serder.ser(object[i]).get());
		writing(messages);
	}

	@SafeVarargs
	public final <E> void write(String topic, Converter<E, byte[]> keying, E... object) {
		Message[] messages = new Message[object.length];
		for (int i = 0; i < object.length; i++)
			messages[i] = new Message(topic, keying.apply(object[i]), serder.ser(object[i]).get());
		writing(messages);
	}

	public void close() {
		closed.set(true);
		connect.close();
		context.close();
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
		Concurrents.submit(new OutputThread(context, p, config.getBatchSize()));
		return p;
	}

	class OutputThread extends Thread {
		private Queue context;
		private Producer<byte[], byte[]> producer;
		private long batchSize;

		OutputThread(Queue context, Producer<byte[], byte[]> p, long batchSize) {
			super();
			this.context = context;
			this.producer = p;
			this.batchSize = batchSize;
		}

		@Override
		public void run() {
			while (!closed.get()) {
				List<Message> msgs = context.dequeue(batchSize);
				List<KeyedMessage<byte[], byte[]>> l = new ArrayList<>(msgs.size());
				for (Message m : msgs)
					l.add(new KeyedMessage<byte[], byte[]>(m.getTopic(), m.getKey(), m.getMessage()));
				producer.send(l);
				logger.trace(() -> "Kafka service sent (amount: [" + msgs.size() + "]).");
			}
		}
	}
}
