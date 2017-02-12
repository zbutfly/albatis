package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.utils.ZkUtils;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Maps;
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

	@SuppressWarnings("unchecked")
	private <T> T fetchValue(String path) {
		String text = fetchText(path);
		return null == text ? null : (T) JsonSerder.JSON_OBJECT.der(text);
	}

	private Map<String, Object> fetchMap(String path) {
		String text = fetchText(path);
		return null == text ? null : JsonSerder.JSON_MAPPER.der(text);
	}

	public String[] getBorkers() {
		return fetchChildren(ZkUtils.BrokerIdsPath()).stream().map(bid -> {
			Pair<String, Integer> addr = getBorkerAddr(Integer.parseInt(bid));
			return addr.v1() + ":" + addr.v2().toString();
		}).collect(Collectors.toList()).toArray(new String[0]);
	}

	private Pair<String, Integer> getBorkerAddr(int id) {
		Map<String, Object> info = fetchMap(ZkUtils.BrokerIdsPath() + "/" + id);
		return null == info ? null : new Pair<String, Integer>((String) info.getOrDefault("host", "127.0.0.1"), (Integer) info.get("port"));
	}

	public Map<String, int[]> getTopicPartitions() {
		@SuppressWarnings("unchecked")
		Stream<Tuple2<String, Set<Integer>>> s = fetchChildren(ZkUtils.BrokerTopicsPath()).stream().map(topic -> {
			Map<String, List<Integer>> parts = (Map<String, List<Integer>>) fetchMap(ZkUtils.getTopicPath(topic)).get("partitions");
			Tuple2<String, Set<Integer>> t = new Tuple2<String, Set<Integer>>(topic, parts.keySet().stream().map(i -> Integer.parseInt(i))
					.collect(Collectors.toSet()));
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
		return Stream.of(topics).collect(Collectors.toMap(t -> t, t -> {
			Map<String, Object> info = fetchMap(ZkUtils.getTopicPath(t));
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
		SimpleConsumer c = getLeaderConsumer(topic, group);
		try {
			return IntStream.of(getTopicPartitions(topic).get(topic)).mapToLong(p -> getLag(c, topic, group, p)).sum();
		} finally {
			c.close();
		}
	}

	private long getLag(SimpleConsumer consumer, String topic, String group, int part) {
		long[] logsize = consumer.getOffsetsBefore(new OffsetRequest(Maps.of(new TopicAndPartition(topic, part),
				new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1)), kafka.api.OffsetRequest.CurrentVersion(), group))
				.offsets(topic, part);
		return logsize.length == 0 ? 0 : logsize[0] - getOffset(topic, group, part);
	}

	private long getOffset(String topic, String group, int part) {
		String text = fetchText("/consumers/" + group + "/offsets/" + topic + "/" + part);
		return null == text ? 0 : Long.parseLong(text);
	}

	@SuppressWarnings("unchecked")
	@Deprecated
	protected <T> T fetchTree(String path) {
		List<String> nodes = fetchChildren(path);
		if (null == nodes || nodes.isEmpty()) {
			Map<String, Object> map = fetchMap(path);
			return null == map ? fetchValue(path) : (T) map;
		} else return (T) nodes.parallelStream().map(node -> {
			String subpath = "/".equals(path) ? "/" + node : path + "/" + node;
			System.err.println("Scan zk: " + subpath);
			Object sub = fetchTree(subpath);
			return new Pair<String, Object>(node, sub);
		}).filter(p -> p.v2() != null).collect(Pair.toMap());
	}

	@Override
	public void close() {
		if (null != zk) try {
			zk.close();
		} catch (InterruptedException e) {
			logger.warn("Kafka closing [" + zk.toString() + "]  interrupted and ignored.");
		}
	}

	private SimpleConsumer getLeaderConsumer(String topic, String group) {
		int leader = ((Integer) fetchMap("/brokers/topics/" + topic + "/partitions/0/state").get("leader")).intValue();
		Pair<String, Integer> addr = getBorkerAddr(leader);
		return new SimpleConsumer(addr.v1(), addr.v2(), 500, 64 * 1024, group);
	}

	public static void main(String[] args) throws NumberFormatException, KeeperException, InterruptedException {
		String topic = "HZGA_GAZHK_LGY_NB";
		String group = "HbaseFromKafkaTest_1";
		try (ZKConn zk = new ZKConn("hzga136:2181,hzga137:2181,hzga138:2181/kafka")) {
			System.out.println(zk.getTopicPartitions());
			System.out.println(zk.getTopicPartitions(topic));
			System.out.println(group + "@" + topic + ":" + zk.getLag(topic, group));
			System.out.println(ZkUtils.apply("hzga136:2181,hzga137:2181,hzga138:2181/kafka", 500, 500, false).getConsumerPartitionOwnerPath(
					group, topic, 0));
		}
	}
}