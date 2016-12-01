package net.butfly.albatis.kafka.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;

import kafka.utils.ZkUtils;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.logger.Logger;

@SuppressWarnings("unchecked")
public final class Kafkas extends Utils {
	private final static Logger logger = Logger.getLogger(Kafkas.class);

	private Kafkas() {}

	public static String[] getBorkers(String zkconn) {
		List<String> brokens = new ArrayList<>();
		ZkClient zk = ZkUtils.createZkClient(zkconn, 500, 500);
		try {
			for (String brokenId : zk.getChildren(ZkUtils.BrokerIdsPath())) {
				Map<String, Object> info = JsonSerder.JSON_MAPPER.der(zk.readData(ZkUtils.BrokerIdsPath() + "/" + brokenId));
				brokens.add(info.get("host") + ":" + info.get("port"));
			}
		} finally {
			zk.close();
		}
		return brokens.toArray(new String[brokens.size()]);
	}

	public static Map<String, Integer> getAllTopicInfo(String zkconn) {
		Map<String, Integer> topics = new HashMap<>();
		ZkClient zk = ZkUtils.createZkClient(zkconn, 500, 500);
		try {
			for (String topic : zk.getChildren(ZkUtils.BrokerTopicsPath())) {
				Map<String, Object> info = JsonSerder.JSON_MAPPER.der(zk.readData(ZkUtils.BrokerTopicsPath() + "/" + topic));;
				topics.put(topic, ((Map<Integer, int[]>) info.get("partitions")).keySet().size());
			}
		} finally {
			zk.close();
		}
		return topics;
	}

	public static Map<String, Integer> getTopicInfo(String zkconn, String... topic) {
		if (topic == null || topic.length == 0) return getAllTopicInfo(zkconn);
		Map<String, Integer> topics = new HashMap<>();
		ZkClient zk = ZkUtils.createZkClient(zkconn, 500, 500);
		try {
			for (String t : topic) {
				try {
					Map<String, Object> info = JsonSerder.JSON_MAPPER.der(zk.readData(ZkUtils.BrokerTopicsPath() + "/" + t));;
					topics.put(t, ((Map<Integer, int[]>) info.get("partitions")).keySet().size());
				} catch (Exception e) {
					logger.error("Topic info fetch failure, return topic with count -1", e);
					topics.put(t, -1);
				}
			}
		} finally {
			zk.close();
		}
		return topics;
	}

	public static void main(String[] args) {
		System.out.println(getAllTopicInfo("hzga136:2181,hzga137:2181,hzga138:2181/kafka"));
	}
}
