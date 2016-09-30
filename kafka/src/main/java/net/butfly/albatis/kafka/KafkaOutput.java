package net.butfly.albatis.kafka;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.javaapi.producer.Producer;
import net.butfly.albacore.io.Output;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.serder.BsonSerder;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.kafka.Queue.Message;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;

public class KafkaOutput implements Output<Message> {
	private static final long serialVersionUID = -276336973758504567L;
	private static final Logger logger = Logger.getLogger(KafkaOutput.class);

	private final Queue context;
	private final ExecutorService threads;
	private final BsonSerder serder;
	private final Producer<byte[], byte[]> connect;

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
		this.serder = new BsonSerder();
		threads = Executors.newFixedThreadPool(1);
		context = new Queue(curr(config.toString()), config.getPoolSize());
		logger.trace("Writing threads pool created (max: " + 1 + ").");
		this.connect = connect(threads, config);
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
		threads.shutdown();
		context.close();
		connect.close();
	}

	private String curr(String folder) throws KafkaException {
		try {
			String path = "./.queue/" + Reflections.getMainClass().getSimpleName() + "/" + folder.replaceAll("[:/\\,]", "-");
			File f = new File(path);
			f.mkdirs();
			return f.getCanonicalPath();
		} catch (IOException e) {
			throw new KafkaException(e);
		}
	}

	private Producer<byte[], byte[]> connect(ExecutorService threads, KafkaOutputConfig config) {
		Producer<byte[], byte[]> p;
		try {
			p = new Producer<>(config.getConfig());
		} catch (KafkaException e) {
			throw new RuntimeException(e);
		}
		threads.submit(new OutputThread(context, p, config.getBatchSize()));
		return p;
	}
}
