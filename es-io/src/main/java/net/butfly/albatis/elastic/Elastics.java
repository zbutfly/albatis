package net.butfly.albatis.elastic;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.logger.Logger;

@Deprecated
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
}
