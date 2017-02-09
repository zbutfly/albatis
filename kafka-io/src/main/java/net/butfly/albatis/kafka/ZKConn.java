package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

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
			return new String(zk.getData(path, false, null));
		} catch (KeeperException e) {
			logger.error("ZK failure", e);
			return null;
		} catch (InterruptedException e) {
			logger.error("Kafka connecting [" + zk.toString() + "] path [" + path + "] interrupted.", e);
			return null;
		}
	}

	private Map<String, Object> fetchJSON(String path) {
		String text = fetchText(path);
		return null == text ? null : JsonSerder.JSON_MAPPER.der(text);
	}

	public String[] getBorkers() throws IOException {
		return fetchChildren(ZkUtils.BrokerIdsPath()).stream().map(brokenId -> {
			Map<String, Object> info = fetchJSON(ZkUtils.BrokerIdsPath() + "/" + brokenId);
			return info.get("host") + ":" + info.get("port");
		}).collect(Collectors.toList()).toArray(new String[0]);
	}

	public Map<String, int[]> getTopicPartitions() {
		@SuppressWarnings("unchecked")
		Stream<Tuple2<String, Set<Integer>>> s = fetchChildren(ZkUtils.BrokerTopicsPath()).stream().map(
				topic -> new Tuple2<String, Set<Integer>>(topic, ((Map<Integer, int[]>) fetchJSON(ZkUtils.getTopicPath(topic)).get(
						"partitions")).keySet()));
		return s.collect(Collectors.toMap(t -> t._1, t -> t._2.stream().mapToInt(i -> i).sorted().toArray()));
	}

	public Map<String, int[]> getTopicPartitions(String... topics) {
		if (topics == null || topics.length == 0) return getTopicPartitions();
		return Arrays.asList(topics).stream().collect(Collectors.toMap(t -> t, t -> {
			Map<String, Object> info = fetchJSON(ZkUtils.getTopicPath(t));
			if (null == info) return new int[0];
			else {
				// ZkUtils.apply("hzga136:2181,hzga137:2181,hzga138:2181/kafka",
				// 500, 500, false).getConsumerPartitionOwnerPath(
				// "HbaseFromKafkaTest_1", "HZGA_GAZHK_ZZRK", 0);
				// List<String> parts = getChildren(zk,
				// "/brokers/topics/HZGA_GAZHK_LGY_NB/partitions/0/state");
				// Integer.parseInt(new String(new
				// ZKConn("hzga136:2181,hzga137:2181,hzga138:2181/kafka").zk.getData(
				// "/consumers/HbaseFromKafkaTest_1/offsets/HZGA_GAZHK_LGY_NB/0",
				// false, null)));
				// PartitionOffsetRequestInfo poreq = new
				// PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(),
				// 9);
				// SimpleConsumer consumer = new SimpleConsumer(host, port,
				// soTimeout, bufferSize, clientId);
				// // consumer.getOffsetsBefore(request)
				// OffsetRequest oreq = new OffsetRequest();
				//
				// getChildren(new
				// ZKConn("hzga136:2181,hzga137:2181,hzga138:2181/kafka"),
				// "/admin");
				// new String(new
				// ZKConn("hzga136:2181,hzga137:2181,hzga138:2181/kafka").zk.getData("/admin",
				// false, null));

				@SuppressWarnings("unchecked")
				Map<Integer, int[]> counts = (Map<Integer, int[]>) info.get("partitions");
				logger.debug(() -> "Kafka topic [" + t + "] info fetch from zk [" + zk + "]: " + counts.toString());
				return counts.keySet().stream().mapToInt(i -> i).sorted().toArray();
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

	public static void main(String[] args) {
		try (ZKConn zk = new ZKConn("hzga136:2181,hzga137:2181,hzga138:2181/kafka")) {
			System.out.println(zk.getTopicPartitions());
		}
	}
}