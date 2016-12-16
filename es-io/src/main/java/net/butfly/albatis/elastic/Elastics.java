package net.butfly.albatis.elastic;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.logger.Logger;

public final class Elastics extends Utils {
	protected static final Logger logger = Logger.getLogger(Elastics.class);

	public static TransportClient connect(String clusterName, String... hostports) throws UnknownHostException {
		TransportClient elastic = TransportClient.builder().settings(Settings.settingsBuilder().put("cluster.name", clusterName).build())
				.build();
		for (String h : hostports) {
			String[] host = h.split(":");
			int port = host.length > 1 ? 39300 : Integer.parseInt(host[1]);
			elastic.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host[0]), port));
		}
		return elastic;
	}

	public static void indices(TransportClient connect, String type, String index, Set<String> nestedPrefix) {
		PutMappingRequest req = new PutMappingRequest();
		int i = 0;
		StringBuilder buffer = new StringBuilder();
		for (String str : nestedPrefix) {
			String value = ("\"" + str + "\":{\"type\":\"nested\",\"properties\":{\"type\":{\"type\":\"long\"}}}");
			if (++i < nestedPrefix.size()) value = value + ",";
			buffer.append(value);
		}
		req.source("{\"dynamic\":\"true\",\"properties\":{" + buffer.toString() + "}}");
		req.indices(new String[] { index });
		req.type(type);
		PutMappingResponse resp = connect.admin().indices().putMapping(req).actionGet();
		if (!resp.isAcknowledged()) throw new RuntimeException("重新构建mapping失败 请检查构造参数：" + req.toString());
	}
}
