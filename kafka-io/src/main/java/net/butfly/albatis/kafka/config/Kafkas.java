package net.butfly.albatis.kafka.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;

import kafka.utils.ZkUtils;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.Utils;

public final class Kafkas extends Utils {
	private Kafkas() {}

	public static String[] getBorkens(String zkconn) {
		List<String> brokens = new ArrayList<>();
		ZkClient zk = ZkUtils.createZkClient(zkconn, 500, 500);
		try {
			for (String brokenId : zk.getChildren(ZkUtils.BrokerIdsPath())) {
				Map<String, Object> broken = JsonSerder.JSON_MAPPER.der(zk.readData(ZkUtils.BrokerIdsPath() + "/" + brokenId));
				brokens.add(broken.get("host") + ":" + broken.get("port"));
			}
		} finally {
			zk.close();
		}
		return brokens.toArray(new String[brokens.size()]);
	}
}
