package net.butfly.albatis.kafka.config;

import static net.butfly.albacore.paral.Sdream.of;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Logger;

public class ZkConnection implements AutoCloseable {
	protected static final Logger logger = Logger.getLogger(KafkaZkParser.class);
	private static final int MAX_RETRIES = Integer.parseInt(Configs.gets("albatis.kafka.zookeeper.max.retries", "10"));
	private static final Watcher w = e -> {};

	protected final ZooKeeper zk;

	public ZkConnection(String zkconn) throws IOException {
		super();
		zk = new ZooKeeper(zkconn, 500, w);
	}

	public Sdream<String> fetchChildren(String path) {
		int retried = 0;
		KeeperException err = null;
		try {
			while (retried++ < MAX_RETRIES) try {
				return of(zk.getChildren(path, false));
			} catch (KeeperException e) {
				logger.warn("ZK children [" + path + "] being retried [" + retried + "] for failure: " + e.toString());
				err = e;
				Thread.sleep(100);
			}
		} catch (InterruptedException e) {
			throw new RuntimeException("Kafka connecting [" + zk.toString() + "] path [" + path + "] interrupted.", e);
		}
		logger.error("ZK children [" + path + "] failure after [" + retried + "] retries.", err);
		return of();
	}

	public String fetchText(String path) {
		int retried = 0;
		KeeperException err = null;
		try {
			while (retried++ < MAX_RETRIES) try {
				byte[] b = zk.getData(path, false, null);
				return null == b ? null : new String(b);
			} catch (KeeperException e) {
				logger.warn("ZK data [" + path + "] retry [" + retried + "] for failure: " + e.toString());
				err = e;
				Thread.sleep(100);
			}
		} catch (InterruptedException e) {
			logger.warn("Kafka connecting [" + zk.toString() + "] path [" + path + "] interrupted." + e.getMessage());
			return null;
		}
		if (null != err) logger.warn("ZK data [" + path + "] failed after [" + retried + "] retries with error: " + err.toString());
		return null;
	}

	public <T> T fetchValue(String path, Class<T> cl) {
		String text = fetchText(path);
		return null == text ? null : JsonSerder.SERDER(cl).der(text, cl);
	}

	public Map<String, Object> fetchMap(String path) {
		String text = fetchText(path);
		return null == text ? null : JsonSerder.JSON_MAPPER.der(text);
	}

	@SuppressWarnings("unchecked")
	@Deprecated
	protected <T> T fetchTree(String path, Class<T> cl) {
		List<String> nodes = fetchChildren(path).list();
		if (Colls.empty(nodes)) {
			Map<String, Object> map = fetchMap(path);
			return null == map ? fetchValue(path, cl) : (T) map;
		} else return (T) of(nodes).partitions(n -> n, node -> {
			String subpath = "/".equals(path) ? "/" + node : path + "/" + node;
			System.err.println("Scan zk: " + subpath);
			Object sub = fetchTree(subpath, cl);
			return new Pair<String, Object>(node, sub);
		});
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
