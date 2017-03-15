package net.butfly.albatis.elastic;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.logger.Logger;

public final class Elastics extends Utils {
	protected static final Logger logger = Logger.getLogger(Elastics.class);

	@Deprecated
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

	public static void writeScript(ObjectOutputStream oos, Script script) throws IOException {
		oos.writeUTF(script.getIdOrCode());
		oos.writeUTF(script.getType().name());
		oos.writeUTF(script.getLang());
		oos.writeObject(script.getParams());
	}

	@SuppressWarnings("unchecked")
	public static Script readScript(ObjectInputStream oos) throws IOException {
		String script = oos.readUTF();
		ScriptType st = ScriptType.valueOf(oos.readUTF());
		String lang = oos.readUTF();
		try {
			return new Script(st, lang, script, (Map<String, Object>) oos.readObject());
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
	}

	public static void mapping(ElasticConnection connect, Map<String, ?> mapping, String... types) {
		logger.debug("Mapping constructing: " + mapping);
		PutMappingRequest req = new PutMappingRequest(connect.getDefaultIndex());
		req.source(mapping);
		Set<String> tps = new HashSet<>(Arrays.asList(types));
		tps.add(connect.getDefaultType());
		for (String t : tps) {
			req.type(t);
			PutMappingResponse resp = connect.client().admin().indices().putMapping(req).actionGet();
			if (!resp.isAcknowledged()) logger.error("Mapping failed" + req.toString());
		}
	}
}
