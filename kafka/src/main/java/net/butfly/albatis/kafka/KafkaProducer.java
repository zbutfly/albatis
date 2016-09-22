package net.butfly.albatis.kafka;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.producer.Producer;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.serder.BsonSerder;
import net.butfly.albatis.impl.kafka.config.KafkaProducerConfig;

@SuppressWarnings("deprecation")
public class KafkaProducer implements AutoCloseable {
	private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

	private String name;
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
	public KafkaProducer(final String name, final KafkaProducerConfig config, int batchSize) throws KafkaException {
		super();
		this.name = name;
		this.serder = new BsonSerder();
		threads = Executors.newFixedThreadPool(1);
		context = new QueueMixed(curr(config.toString()), batchSize);
		logger.info("[" + name + "] Producer thread starting (max: " + 1 + ")...");
		createProcuder(threads, config, batchSize);
	}

	@SuppressWarnings("unchecked")
	public void write(String topic, Map<String, Object> meta, Map<String, Object>... map) {
		for (Map<String, Object> m : map)
			context.enqueue(topic, serder.ser(new KafkaMapWrapper(null, meta, m)));
	}

	@SuppressWarnings("unchecked")
	public void write(String topic, Map<String, Object> meta, Converter<Map<String, Object>, String> keying, Map<String, Object>... map) {
		for (Map<String, Object> m : map)
			context.enqueue(topic, serder.ser(new KafkaMapWrapper(null, meta, m)));
	}

	@SuppressWarnings("unchecked")
	public <E> void write(String topic, E... object) {
		for (E e : object)
			context.enqueue(topic, serder.ser(new KafkaObjectWrapper<E>(null, e)));
	}

	@SuppressWarnings("unchecked")
	public <E> void write(String topic, Converter<E, String> keying, E... object) {
		for (E e : object)
			context.enqueue(topic, serder.ser(new KafkaObjectWrapper<E>(null, e)));
	}

	public void close() {
		logger.info("[" + name + "] All threads closing...");
		threads.shutdown();
		logger.info("[" + name + "] Buffer clearing...");
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

	private void createProcuder(ExecutorService threads, KafkaProducerConfig config, int batchSize) {
		Producer<String, byte[]> p;
		try {
			p = new Producer<>(config.getConfig());
		} catch (KafkaException e) {
			throw new RuntimeException(e);
		}
		threads.submit(new ProducerThread(context, p));
	}
}
