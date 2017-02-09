package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.utils.ZkUtils;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.logger.Logger;
import scala.Tuple2;

public class ZKConn implements AutoCloseable {
	private final static Logger logger = Logger.getLogger(ZKConn.class);
	final ZooKeeper zk;

	public ZKConn(String zkconn) {
		super();
		try {
			this.zk = new ZooKeeper(zkconn, 500, e -> {});
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private List<String> fetchChildren(String path) {
		try {
			return zk.getChildren(path, false);
		} catch (KeeperException e) {
			throw new RuntimeException("ZK failure", e);
		} catch (InterruptedException e) {
			throw new RuntimeException("Kafka connecting [" + zk.toString() + "] path [" + path + "] interrupted.", e);
		}
	}

	private String fetchText(String path) {
		try {
			byte[] b = zk.getData(path, false, null);
			return null == b ? null : new String(b);
		} catch (KeeperException e) {
			logger.error("ZK failure", e);
			return null;
		} catch (InterruptedException e) {
			logger.error("Kafka connecting [" + zk.toString() + "] path [" + path + "] interrupted.", e);
			return null;
		}
	}

	private Map<String, Object> fetch(String path) {
		String text = fetchText(path);
		return null == text ? null : JsonSerder.JSON_MAPPER.der(text);
	}

	public String[] getBorkers() throws IOException {
		return fetchChildren(ZkUtils.BrokerIdsPath()).stream().map(brokenId -> {
			Map<String, Object> info = fetch(ZkUtils.BrokerIdsPath() + "/" + brokenId);
			return info.get("host") + ":" + info.get("port");
		}).collect(Collectors.toList()).toArray(new String[0]);
	}

	public Map<String, int[]> getTopicPartitions() {
		@SuppressWarnings("unchecked")
		Stream<Tuple2<String, Set<Integer>>> s = fetchChildren(ZkUtils.BrokerTopicsPath()).stream().map(topic -> {
			Tuple2<String, Set<Integer>> t = new Tuple2<String, Set<Integer>>(topic, ((Map<String, List<Integer>>) fetch(ZkUtils
					.getTopicPath(topic)).get("partitions")).keySet().stream().map(i -> Integer.parseInt(i)).collect(Collectors.toSet()));
			return t;
		});
		return s.collect(Collectors.toMap(t -> {
			return t._1;
		}, t -> {
			IntStream ss = t._2.stream().mapToInt(i -> {
				return null == i ? 0 : i.intValue();
			});
			return ss.sorted().toArray();
		}));
	}

	public Map<String, int[]> getTopicPartitions(String... topics) {
		if (topics == null || topics.length == 0) return getTopicPartitions();
		return Arrays.asList(topics).stream().collect(Collectors.toMap(t -> t, t -> {
			Map<String, Object> info = fetch(ZkUtils.getTopicPath(t));
			if (null == info) return new int[0];
			else {
				@SuppressWarnings("unchecked")
				Map<String, List<Integer>> counts = (Map<String, List<Integer>>) info.get("partitions");
				logger.debug(() -> "Kafka topic [" + t + "] info fetch from zk [" + zk + "]: " + counts.toString());
				new ArrayList<>(counts.keySet()).get(0);
				return counts.keySet().stream().mapToInt(i -> {
					return Integer.parseInt(i);
				}).sorted().toArray();
			}
		}));
	}

	public long getLag(String topic, String group) {
		return IntStream.of(getTopicPartitions(topic).get(topic)).mapToLong(p -> getLag(topic, group, p)).sum();
	}

	public long getLag(String topic, String group, int part) {
		return 0;
	}

	public long getOffset(String topic, String group) {
		return IntStream.of(getTopicPartitions(topic).get(topic)).mapToLong(p -> getOffset(topic, group, p)).sum();
	}

	public long getOffset(String topic, String group, int part) {
		String text = fetchText("/consumers/" + group + "/offsets/" + topic + "/" + part);
		return null == text ? 0 : Long.parseLong(text);
	}

	@Override
	public void close() {
		if (null != zk) try {
			zk.close();
		} catch (InterruptedException e) {
			logger.warn("Kafka closing [" + zk.toString() + "]  interrupted and ignored.");
		}
	}

	public static void main(String[] args) throws NumberFormatException, KeeperException, InterruptedException {
		try (ZKConn zk = new ZKConn("hzga136:2181,hzga137:2181,hzga138:2181/kafka")) {
			System.out.println(zk.getTopicPartitions());
			String topic = "HZGA_GAZHK_LGY_NB";
			System.out.println(ZkUtils.apply("hzga136:2181,hzga137:2181,hzga138:2181/kafka", 500, 500, false).getConsumerPartitionOwnerPath(
					"HbaseFromKafkaTest_1", "HZGA_GAZHK_ZZRK", 0));
			System.out.println(zk.fetch("/brokers/topics/" + topic + "/partitions/0/state"));
			System.out.println(Integer.parseInt(zk.fetchText("/consumers/HbaseFromKafkaTest_1/offsets/" + topic + "/0")));
			System.out.println(zk.getTopicPartitions(topic));
			System.out.println(zk.getOffset(topic, "HbaseFromKafkaTest_1"));

			PartitionOffsetRequestInfo poreq = new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 9);
			// SimpleConsumer consumer = new SimpleConsumer(host, port,
			// soTimeout, bufferSize, clientId);
			// consumer.getOffsetsBefore(request)
			// OffsetRequest oreq = new OffsetRequest();
		}
	}
}