package net.butfly.albatis.kafka.config;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import kafka.utils.ZkUtils;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.logger.Logger;

@SuppressWarnings("unchecked")
public final class Kafkas extends Utils {
	private final static Logger logger = Logger.getLogger(Kafkas.class);

	private Kafkas() {}

	public static String[] getBorkers(String zkconn) throws IOException {
		List<String> brokens = new ArrayList<>();
		ZooKeeper zk = new ZooKeeper(zkconn, 500, e -> {});
		try {
			for (String brokenId : zk.getChildren(ZkUtils.BrokerIdsPath(), false)) {
				byte[] d = zk.getData(ZkUtils.BrokerIdsPath() + "/" + brokenId, false, null);
				Map<String, Object> info = JsonSerder.JSON_MAPPER.der(new String(d));
				brokens.add(info.get("host") + ":" + info.get("port"));
			}
		} catch (KeeperException e) {
			throw new IOException(e);
		} catch (InterruptedException e) {} finally {
			try {
				zk.close();
			} catch (InterruptedException e) {}
		}
		return brokens.toArray(new String[brokens.size()]);
	}

	public static Map<String, Integer> getAllTopicInfo(String zkconn) throws IOException {
		Map<String, Integer> topics = new HashMap<>();
		ZooKeeper zk = new ZooKeeper(zkconn, 500, e -> {});
		try {
			for (String topic : zk.getChildren(ZkUtils.BrokerTopicsPath(), false)) {
				byte[] d = zk.getData(ZkUtils.BrokerTopicsPath() + "/" + topic, false, null);
				Map<String, Object> info = JsonSerder.JSON_MAPPER.der(new String(d));
				topics.put(topic, ((Map<Integer, int[]>) info.get("partitions")).keySet().size());
			}
		} catch (KeeperException e) {
			throw new IOException(e);
		} catch (InterruptedException e) {} finally {
			try {
				zk.close();
			} catch (InterruptedException e) {}
		}
		return topics;
	}

	public static Map<String, Integer> getTopicInfo(String zkconn, String... topic) throws IOException {
		if (topic == null || topic.length == 0) return getAllTopicInfo(zkconn);
		Map<String, Integer> topics = new HashMap<>();
		ZooKeeper zk = new ZooKeeper(zkconn, 500, e -> {});
		try {
			for (String t : topic) {
				try {
					byte[] d = zk.getData(ZkUtils.BrokerTopicsPath() + "/" + t, false, null);
					Map<String, Object> info = JsonSerder.JSON_MAPPER.der(new String(d));
					Map<Integer, int[]> counts = (Map<Integer, int[]>) info.get("partitions");
					logger.debug(() -> "Kafka topic [" + t + "] info fetch from zk [" + zkconn + "]: " + counts.toString());
					topics.put(t, counts.keySet().size());
				} catch (Exception e) {
					logger.error("Topic info fetch failure, return topic with count -1", e);
					topics.put(t, -1);
				}
			}
		} finally {
			try {
				zk.close();
			} catch (InterruptedException e) {}
		}
		return topics;
	}

	public static void main(String[] args) throws IOException {
		System.out.println(getAllTopicInfo("hzga136:2181,hzga137:2181,hzga138:2181/kafka"));
	}
}
