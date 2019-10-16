package net.butfly.albatis.kafka;

import static net.butfly.albacore.utils.collection.Colls.list;
import static net.butfly.albatis.ddl.TableDesc.dummy;
import static net.butfly.albatis.io.IOProps.propI;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.kafka.config.Kafka2InputConfig;

public class Kafka2Input extends Namedly implements Input<Rmap> {
	private static final long serialVersionUID = 998704625489437241L;

	private final Kafka2InputConfig config;
	private final Consumer<byte[], byte[]> connect;

	// for debug
	private final AtomicLong skip;

	public Kafka2Input(String name, String kafkaURI, TableDesc... topics) throws ConfigException, IOException {
		this(name, new URISpec(kafkaURI), topics);
	}

	public Kafka2Input(String name, URISpec kafkaURI) throws ConfigException, IOException {
		this(name, kafkaURI, dummy(topic(kafkaURI)).toArray(new TableDesc[0]));
	}

	protected static String[] topic(URISpec kafkaURI) {
		String topics = kafkaURI.getParameter("topic", kafkaURI.getFile());
		if (null == topics) throw new RuntimeException("Kafka topic not defined, as File segment of uri or [topic=TOPIC1,TOPIC2,...]");
		return Texts.split(topics, ",").toArray(new String[0]);
	}

	public Kafka2Input(String name, URISpec uri, TableDesc... topics) throws ConfigException, IOException {
		super(name);
		config = new Kafka2InputConfig(name(), uri);
		skip = new AtomicLong(Long.parseLong(uri.getParameter("skip", "0")));
		if (skip.get() > 0) logger().error("[" + name() + "] skip [" + skip.get()
				+ "] for testing, the skip is estimated, especially in multiple topic subscribing.");
		int configTopicParallinism = propI(Kafka2Input.class, "topic.paral", config.getDefaultPartitionParallelism());
		if (configTopicParallinism > 0) //
			logger().debug("[" + name() + "] default topic parallelism [" + configTopicParallinism + "]");
		if (topics == null || topics.length == 0) topics = dummy(config.topics()).toArray(new TableDesc[0]);

		connect = new KafkaConsumer<>(config.props());
		connect.subscribe(list(t -> t.qualifier.name, topics));
		closing(this::closeKafka);
	}

	@Override
	public Statistic statistic() {
		return new Statistic(this).<ConsumerRecord<byte[], byte[]>> sizing(km -> (long) km.value().length) //
				.<ConsumerRecord<byte[], byte[]>> infoing(km -> new String(km.key())).detailing(Exeter.of()::toString);
	}

	private static final Duration TIMEOUT = Duration.of(Long.parseLong(Configs.gets("albatis.kafka.input.timeout", "100")), ChronoUnit.MILLIS);

	@Override
	public void dequeue(net.butfly.albacore.io.lambda.Consumer<Sdream<Rmap>> using) {
		ConsumerRecords<byte[], byte[]> it;
		while (opened()) {
			synchronized (connect) {
				try {
					it = connect.poll(TIMEOUT);
				} catch (IllegalStateException e) {
					logger().error("Illegal state when poll data by consumer", e);
					it = null;
				}
			}
			if (Colls.empty(it)) continue;
			using.accept(Sdream.of(list(it, km -> {
				ConsumerRecord<byte[], byte[]> kmm = s().stats(km);
				String k = null == kmm.key() || kmm.key().length == 0 ? null : new String(kmm.key());
				return new Rmap(kmm.topic(), k, null == k ? "km" : k, (Object) kmm.value());
			})));
			return;
		}
	}

	private void closeKafka() {
		synchronized (connect) {
			try {
				connect.commitSync();
			} catch (Exception e) {
				logger().error("[" + name() + "] commit fail", e);
			}
			try {
				connect.close();
			} catch (Exception e) {
				logger().error("[" + name() + "] shutdown fail", e);
			}
		}
	}
}
