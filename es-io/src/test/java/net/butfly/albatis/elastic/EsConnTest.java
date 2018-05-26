package net.butfly.albatis.elastic;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.logger.Logger;

public class EsConnTest {
	public static final Logger logger = Logger.getLogger(EsConnTest.class);
	private static final String MAPPING_FILE = "C:\\Workspaces\\dataggr\\dataggr\\pumps\\subject\\src\\test\\scripts\\es-mapping.json";

	public static void main(String[] args) throws IOException {
		InetSocketAddress addr = new InetSocketAddress("hzwa130", 39200);
		Map<String, Object> meta = ElasticConnect.Parser.fetchMetadata(addr);
		System.err.println("Cluster name: " + (String) meta.get("cluster_name"));
		for (ElasticConnect.Node r : ElasticConnect.Parser.getNodes(meta))
			System.err.println(r.toString());
	}

	public static RestClient rest() {
		return RestClient.builder(HttpHost.create("http://hzwa130:39200")).build();
	}

	public static Client transport() {
		Settings.Builder settings = Settings.builder();
		settings.put("cluster.name", "cominfo");
		// settings.put("client.transport.ignore_cluster_name", true);
		try (TransportClient tc = new PreBuiltTransportClient(settings.build());) {
			return tc.addTransportAddresses(new TransportAddress(new InetSocketAddress("localhost", 39300)));
		}
	}

	public static void testConn() throws IOException {
		try (ElasticConnection conn = new ElasticConnection("es://cominfo@hzga152/person_test/person");) {
			// conn.construct(mapping());
		}
	}

	public static Map<String, Object> mapping() {
		StringBuilder sb = new StringBuilder();
		String l;
		try (FileReader fr = new FileReader(MAPPING_FILE); BufferedReader r = new BufferedReader(fr);) {
			while ((l = r.readLine()) != null)
				sb.append(l);
		} catch (FileNotFoundException e) {
			return null;
		} catch (IOException e) {
			return null;
		}
		return JsonSerder.JSON_MAPPER.der(sb);

	}
}
