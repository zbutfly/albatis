package net.butfly.albatis.kafka;

import static net.butfly.albatis.ddl.TableDesc.dummy;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.exception.ConfigException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.kafka.config.Kafka2InputConfig;

public class Kafka2Input extends Namedly implements KafkaIn {
	private static final long serialVersionUID = 998704625489437241L;

	private final Kafka2InputConfig config;
	private final Map<String, Integer> allTopics = Maps.of();
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
		int configTopicParallinism = Props.propI(Kafka2Input.class, "topic.paral", config.getDefaultPartitionParallelism());
		if (configTopicParallinism > 0) //
			logger().debug("[" + name() + "] default topic parallelism [" + configTopicParallinism + "]");
		if (topics == null || topics.length == 0) topics = dummy(config.topics()).toArray(new TableDesc[0]);

		connect = new KafkaConsumer<>(config.props());
		connect.subscribe(Colls.list(allTopics.keySet()));
		closing(this::closeKafka);
	}

	@Override
	public Statistic trace() {
		return new Statistic(this).<ConsumerRecord<byte[], byte[]>> sizing(km -> (long) km.value().length) //
				.<ConsumerRecord<byte[], byte[]>> sampling(km -> new String(km.key())).detailing(Exeter.of()::toString);
	}

	private static final Duration TIMEOUT = Duration.of(Long.parseLong(Configs.gets("albatis.kafka.input.timeout", "100")),
			ChronoUnit.MILLIS);

	@Override
	public Rmap dequeue() {
		ConsumerRecords<byte[], byte[]> it;
		while (opened())
			if (!Colls.empty(it = connect.poll(TIMEOUT))) {
				try {
					List<Rmap> rs = Colls.list();
					for (ConsumerRecord<byte[], byte[]> km : it) {
						km = s().stats(km);
						String k = null == km.key() || km.key().length == 0 ? null : new String(km.key());
						rs.add(new Rmap(km.topic(), k, null == k ? "km" : k, (Object) km.value()));

					}
				} catch (Exception ex) {
					logger().warn("Unprocessed kafka error [" + ex.getClass().toString() + ": " + ex.getMessage()
							+ "], ignore and continue.", ex);
					return null;
				}
			}
		return null;
	}

	private void closeKafka() {
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
