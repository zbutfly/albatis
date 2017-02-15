package net.butfly.albatis.elastic;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;

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
		oos.writeUTF(script.getScript());
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
			return new Script(script, st, lang, (Map<String, ? extends Object>) oos.readObject());
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
	}
}
