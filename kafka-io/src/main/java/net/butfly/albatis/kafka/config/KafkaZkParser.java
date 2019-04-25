package net.butfly.albatis.kafka.config;

import static net.butfly.albacore.paral.Sdream.of;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.IntStream;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.utils.ZkUtils;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Maps;

public class KafkaZkParser extends ZkConnection {
	public KafkaZkParser(String zkconn) throws IOException {
		super(zkconn);
	}

	public String[] getBrokers() {
		return fetchChildren(ZkUtils.BrokerIdsPath()).map(bid -> {
			Pair<String, Integer> addr = getBrokerAddr(Integer.parseInt(bid));
			return addr.v1() + ":" + addr.v2().toString();
		}).list().toArray(new String[0]);
	}

	private Pair<String, Integer> getBrokerAddr(int id) {
		Map<String, Object> info = fetchMap(ZkUtils.BrokerIdsPath() + "/" + id);
		return null == info ? null : new Pair<String, Integer>((String) info.getOrDefault("host", "127.0.0.1"), (Integer) info.get("port"));
	}

	public Map<String, int[]> getTopicPartitions() {
		return fetchChildren(ZkUtils.BrokerTopicsPath()).map(topic -> {
			@SuppressWarnings("unchecked")
			Map<String, List<Integer>> parts = (Map<String, List<Integer>>) fetchMap(ZkUtils.getTopicPath(topic)).get("partitions");
			Pair<String, int[]> t = new Pair<>(topic, parts.keySet().stream().mapToInt(Integer::parseInt).sorted().toArray());
			return t;
		}).partitions(t -> t.v1(), t -> t.v2());
	}

	public Map<String, int[]> getTopicPartitions(String... topics) {
		if (topics == null || topics.length == 0) return getTopicPartitions();
		return of(topics).partitions(t -> t, t -> {
			Map<String, Object> info = fetchMap(ZkUtils.getTopicPath(t));
			if (null == info) return new int[0];
			else {
				@SuppressWarnings("unchecked")
				Map<String, List<Integer>> counts = (Map<String, List<Integer>>) info.get("partitions");
				logger.trace(() -> "Kafka topic [" + t + "] info fetch from zk \n\t[" + zk + "]: \n\t" + counts.toString());
				new ArrayList<>(counts.keySet()).get(0);
				return counts.keySet().stream().mapToInt(i -> {
					return Integer.parseInt(i);
				}).sorted().toArray();
			}
		});
	}

	public long getLag(String topic, String group) {
		SimpleConsumer c = getLeaderConsumer(topic, group);
		if (null == c) return -1;
		try {
			int[] s = getTopicPartitions(topic).get(topic);
			if (null == s) return -1;
			return IntStream.of(s).mapToLong(p -> getLag(c, topic, group, p)).sum();
		} catch (Throwable t) {
			return -1;
		} finally {
			c.close();
		}
	}

	private long getLag(SimpleConsumer consumer, String topic, String group, int part) {
		OffsetRequest req = new OffsetRequest(Maps.of(new TopicAndPartition(topic, part), //
				new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1)), kafka.api.OffsetRequest.CurrentVersion(), group);
		OffsetResponse resp = consumer.getOffsetsBefore(req);
		long[] logsize = resp.offsets(topic, part);
		return logsize.length == 0 ? 0 : logsize[0] - getOffset(topic, group, part);
	}

	private long getOffset(String topic, String group, int part) {
		String text = fetchText("/consumers/" + group + "/offsets/" + topic + "/" + part);
		return null == text ? 0 : Long.parseLong(text);
	}

	private SimpleConsumer getLeaderConsumer(String topic, String group) {
		Map<String, Object> m = fetchMap("/brokers/topics/" + topic + "/partitions/0/state");
		if (null == m) return null;
		int leader = ((Integer) m.get("leader")).intValue();
		Pair<String, Integer> addr = getBrokerAddr(leader);
		return new SimpleConsumer(addr.v1(), addr.v2(), 500, 64 * 1024, group);
	}

	public static void main(String[] args) throws IOException {
		String topic = "HZGA_GAZHK_LGY_NB";
		String group = "HbaseFromKafkaTest_1";
		String zkconn = "data01:2181,data02:2181,data03:2181/kafka";
		try (KafkaZkParser zk = new KafkaZkParser(zkconn)) {
			System.out.println(zk.getTopicPartitions());
			System.out.println(zk.getTopicPartitions(topic));
			System.out.println(group + "@" + topic + ":" + zk.getLag(topic, group));
			System.out.println(ZkUtils.apply(zkconn, 500, 500, false).getConsumerPartitionOwnerPath(group, topic, 0));
		}
	}

	static Pair<String, String> bootstrapFromZk(URISpec uri) {
		String path = uri.getPath();
		String[] bstrs = new String[0];
		do {
			try (KafkaZkParser zk = new KafkaZkParser(uri.getHost() + path)) {
				bstrs = zk.getBrokers();
			} catch (Exception e) {
				logger.warn("ZK [" + uri.getHost() + path + "] detecting failure, try parent uri as ZK, error:" + e.toString());
			}
		} while (bstrs.length == 0 && !(path = path.replaceAll("/[^/]*$", "")).isEmpty());
		if (bstrs.length > 0) return new Pair<>(String.join(",", bstrs), uri.getHost() + path);
		else return new Pair<>(Configs.gets(KafkaConfigBase.PROP_PREFIX + "brokers"), uri.getHost() + uri.getPath());
	}

	static String bootstrapFromZk(String zkconn) {
		try (KafkaZkParser zk = new KafkaZkParser(zkconn)) {
			String b = String.join(",", zk.getBrokers());
			logger.trace("ZK [" + zkconn + "] detect broken list automatically: [" + b + "])");
			return b.length() > 0 ? b : null;
		} catch (Exception e) {
			logger.warn("ZK [" + zkconn + "] detect broken list failure: " + e.toString());
			return null;
		}
	}

	static Map<String, Integer> getTopicPartitions(String zk, String gid, String... topics) {
		Map<String, Integer> m = Maps.of();
		if (null != zk) try (KafkaZkParser p = new KafkaZkParser(zk)) {
			Map<String, int[]> topicParts = p.getTopicPartitions(topics);
			for (Entry<String, int[]> e : topicParts.entrySet()) {
				String t = e.getKey();
				m.put(t, e.getValue().length);
				if (logger.isDebugEnabled()) logger.debug("[" + zk + "] lag of " + gid //
						+ "@" + t + ": " + p.getLag(t, gid));
			}
		} catch (IOException e) {
			logger.warn("Topic partition parsing fail on zk [" + zk + "]: " + e.getMessage());
		}
		return m;
	}
}