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
import net.butfly.albatis.kafka.backend.QueueMixed;
import net.butfly.albatis.kafka.config.KafkaOutputConfig;

@SuppressWarnings("deprecation")
public class KafkaOutput implements Output<String> {
	private static final long serialVersionUID = -276336973758504567L;
	private static final Logger logger = LoggerFactory.getLogger(KafkaOutput.class);

	private Queue context;
	private ExecutorService threads;
	private BsonSerder serder;

	/**
	 * @param mixed:
	 *            是否混合模式
	 * @param maxPackageMix:
	 *            混合模式下缓冲区个数
	 * @param maxMessageNoMix:
	 *            缓冲消息个数
	 * @return
	 */
	public KafkaOutput(final String topic, final KafkaOutputConfig config, int batchSize) throws KafkaException {
		super();
		this.serder = new BsonSerder();
		threads = Executors.newFixedThreadPool(1);
		context = new QueueMixed(curr(config.toString()), batchSize);
		logger.info("Producer thread starting (max: " + 1 + ")...");
		createProcuder(threads, config, batchSize);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void write(String topic, Map<String, Object>... message) {
		// TODO: create key
		byte[] k = null;
		for (Map<String, Object> m : message)
			context.enqueue(topic, k, serder.ser(m));
	}

	@SuppressWarnings("unchecked")
	@Override
	public <E> void write(String topic, E... object) {
		// TODO: create key
		byte[] k = null;
		for (E e : object)
			context.enqueue(topic, k, serder.ser(e));
	}

	@SuppressWarnings("unchecked")
	public <E> void write(String topic, Converter<E, byte[]> keying, E... object) {
		for (E e : object)
			context.enqueue(topic, keying.apply(e), serder.ser(e));
	}

	public void close() {
		threads.shutdown();
		context.close();
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

	private void createProcuder(ExecutorService threads, KafkaOutputConfig config, int batchSize) {
		Producer<byte[], byte[]> p;
		try {
			p = new Producer<>(config.getConfig());
		} catch (KafkaException e) {
			throw new RuntimeException(e);
		}
		threads.submit(new OutputThread(context, p));
	}
}
