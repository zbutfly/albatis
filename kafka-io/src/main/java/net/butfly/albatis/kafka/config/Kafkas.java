package net.butfly.albatis.kafka.config;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import kafka.utils.ZkUtils;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.logger.Logger;
import scala.Tuple2;

@SuppressWarnings("unchecked")
public final class Kafkas extends Utils {
	private final static Logger logger = Logger.getLogger(Kafkas.class);

	private Kafkas() {}

	private static List<String> getChildren(ZKConn zk, String path) {
		try {
			return zk.zk.getChildren(path, false);
		} catch (KeeperException e) {
			throw new RuntimeException("ZK failure", e);
		} catch (InterruptedException e) {
			throw new RuntimeException("Kafka connecting [" + zk.toString() + "] path [" + path + "] interrupted.", e);
		}
	}

	private static Map<String, Object> getData(ZKConn zk, String path) {
		try {
			return JsonSerder.JSON_MAPPER.der(new String(zk.zk.getData(path, false, null)));
		} catch (KeeperException e) {
			logger.error("ZK failure", e);
			return null;
		} catch (InterruptedException e) {
			logger.error("Kafka connecting [" + zk.toString() + "] path [" + path + "] interrupted.", e);
			return null;
		}
	}

	public static String[] getBorkers(String zkconn) throws IOException {
		String idsPath = ZkUtils.BrokerIdsPath();
		try (ZKConn zk = new ZKConn(zkconn);) {
			return getChildren(zk, idsPath).stream().map(brokenId -> {
				Map<String, Object> info = getData(zk, idsPath + "/" + brokenId);
				return info.get("host") + ":" + info.get("port");
			}).collect(Collectors.toList()).toArray(new String[0]);
		}
	}

	public static Map<String, Integer> getAllTopicInfo(String zkconn) {
		try (ZKConn zk = new ZKConn(zkconn);) {
			return getChildren(zk, ZkUtils.BrokerTopicsPath()).stream().map(topic -> new Tuple2<String, Integer>(topic,
					((Map<Integer, int[]>) getData(zk, ZkUtils.getTopicPath(topic)).get("partitions")).keySet().size())).collect(Collectors
							.toMap(t -> t._1, t -> t._2));
		}
	}

	public static Map<String, Integer> getTopicInfo(String zkconn, Set<String> topics) {
		if (topics == null || topics.isEmpty()) return getAllTopicInfo(zkconn);
		try (ZKConn zk = new ZKConn(zkconn);) {
			return topics.stream().collect(Collectors.toMap(t -> t, t -> {
				Map<String, Object> info = getData(zk, ZkUtils.getTopicPath(t));
				if (null == info) return -1;
				else {
					Map<Integer, int[]> counts = (Map<Integer, int[]>) info.get("partitions");
					logger.debug(() -> "Kafka topic [" + t + "] info fetch from zk [" + zkconn + "]: " + counts.toString());
					return counts.size();
				}
			}));
		}
	}

	public static void main(String[] args) {
		System.out.println(getAllTopicInfo("hzga136:2181,hzga137:2181,hzga138:2181/kafka"));
	}

	private static class ZKConn implements AutoCloseable {
		final ZooKeeper zk;

		private ZKConn(String zkconn) {
			super();
			try {
				this.zk = new ZooKeeper(zkconn, 500, e -> {});
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void close() {
			if (null != zk) try {
				zk.close();
			} catch (InterruptedException e) {
				logger.warn("Kafka closing [" + zk.toString() + "]  interrupted and ignored.");
			}
		}
	}
}
