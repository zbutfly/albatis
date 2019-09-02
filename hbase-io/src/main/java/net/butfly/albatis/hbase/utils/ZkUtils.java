package net.butfly.albatis.hbase.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Logger;

public class ZkUtils {

	public static Logger logger = Logger.getLogger(ZkUtils.class);
	public static String MASTER_ZNODE_PATH = "/master";
	private static final int MAX_RETRIES = Integer.parseInt(Configs.gets("albatis.hbase.zookeeper.max.retries", "10"));

	public static Pair<String, String> getHbaseZkPath(String hosts, String path) {
		path = path.startsWith("/") ? path.substring(1) : path;
		List<String> zkPath = Colls.list();
		List<String> namespacePath = Colls.list();
		List<String> pathList = new ArrayList<>(Arrays.asList(path.split("/")));
		while (null != pathList) {
			logger.info("Try zk path " + path);
			try {
				ZooKeeper zk = new ZooKeeper(hosts + "/" + String.join("/", pathList), 500, e -> {});
				String data = fetchChildren(zk, MASTER_ZNODE_PATH);
				if (null != data) {
					zkPath.addAll(pathList);
					Pair<String, String> p = new Pair<>();
					p.v1("/" + String.join("/", zkPath));
					p.v2(String.join(":", namespacePath));
					return p;
				}
			} catch (Exception e) {
				logger.warn("zk" + path + " is not available");
			}
			if (!pathList.isEmpty()) {
				namespacePath.add(pathList.remove(pathList.size() - 1));
			} else pathList = null;
//			path = path.replaceAll("/[^/]*$", "");
		}
		return null;
	}

	public static String fetchChildren(ZooKeeper zk, String path) {
		int retried = 0;
		KeeperException err = null;
		try {
			while (retried++ < MAX_RETRIES) try {
				byte[] byteArray = zk.getData(path, false, null);
				return byteArray == null || byteArray.length == 0 ? null : new String(byteArray);
			} catch (KeeperException e) {
				logger.warn("ZK children [" + path + "] being retried [" + retried + "] for failure: " + e.toString());
				err = e;
				Thread.sleep(100);
			}
		} catch (InterruptedException e) {
			throw new RuntimeException("Kafka connecting [" + zk.toString() + "] path [" + path + "] interrupted.", e);
		}
		logger.error("ZK children [" + path + "] failure after [" + retried + "] retries.", err);
		return null;
	}
}
