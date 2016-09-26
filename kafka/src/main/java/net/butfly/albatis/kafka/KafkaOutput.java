package net.butfly.albatis.kafka;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.producer.Producer;
import net.butfly.albacore.io.Output;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.serder.BsonSerder;
import net.butfly.albatis.kafka.backend.OutputThread;
import net.butfly.albatis.kafka.backend.Queue;
import net.butfly.albatis.kafka.backend.Queue.Message;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;

@SuppressWarnings("deprecation")
public class KafkaOutput implements Output<Message> {
	private static final long serialVersionUID = -276336973758504567L;
	private static final Logger logger = LoggerFactory.getLogger(KafkaOutput.class);

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
		logger.info("Producer thread starting (max: " + 1 + ")...");
		this.connect = connect(threads, config);
	}

	@Override
	public void write(Message... message) {
		context.enqueue(message);
	}

	@SafeVarargs
	public final void write(String topic, Converter<Map<String, Object>, byte[]> keying, Map<String, Object>... object) {
		Message[] messages = new Message[object.length];
		for (int i = 0; i < object.length; i++)
			messages[i] = new Message(topic, keying.apply(object[i]), serder.ser(object[i]));
		write(messages);
	}

	@SafeVarargs
	public final <E> void write(String topic, Converter<E, byte[]> keying, E... object) {
		Message[] messages = new Message[object.length];
		for (int i = 0; i < object.length; i++)
			messages[i] = new Message(topic, keying.apply(object[i]), serder.ser(object[i]));
		context.enqueue(messages);
	}

	public void close() {
		threads.shutdown();
		context.close();
		connect.close();
	}

	private String curr(String zookeeperConnect) throws KafkaException {
		try {
			File f = new File("./" + zookeeperConnect.replaceAll("[:/\\,]", "-"));
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
