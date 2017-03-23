package net.butfly.albatis.elastic;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.hzcominfo.albatis.nosql.NoSqlConnection;

import net.butfly.albacore.io.utils.URISpec;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.logger.Logger;

/**
 * @Author Naturn
 *
 * @Date 2017年3月13日-上午10:18:42
 *
 * @Version 1.0.0
 *
 * @Email juddersky@gmail.com
 */

public class ElasticConnection extends NoSqlConnection<TransportClient> {
	private static final Logger logger = Logger.getLogger(ElasticConnection.class);

	protected ElasticConnection(URISpec esUri, Map<String, String> props) throws IOException {
		super(esUri, uri -> {
			Settings.Builder settings = Settings.builder();
			settings.put(uri.getParameters());
			if (null != props && !props.isEmpty()) settings.put(props);
			if (null != uri.getUsername()) settings.put("cluster.name", uri.getUsername());
			else settings.put("client.transport.ignore_cluster_name", true);
			settings.remove("batch");
			settings.remove("script");
			TransportClient tc = new PreBuiltTransportClient(settings.build());
			tc.addTransportAddresses(parseNodes(esUri));
			return tc;
		}, 39300, "es", "elasticsearch", "http", "https");
	}

	private static final Pattern NODE_TRANSPORT_ADDRESS = Pattern.compile("inet\\[/([^:]*):([0-9]+)\\]");

	@SuppressWarnings("unchecked")
	private static InetSocketTransportAddress[] parseNodes(URISpec uri) {
		RestClient rest = RestClient.builder(Arrays.stream(uri.getInetAddrs()).map(a -> new HttpHost(a.getHostName(), a.getPort())).toArray(
				i -> new HttpHost[i])).build();
		Response rep;
		try {
			rep = rest.performRequest("GET", "_nodes");
		} catch (IOException e) {
			logger.warn("Elastic transport meta parsing failed, connect directly", e);
			return uriAddrs(uri);
		}
		if (rep.getStatusLine().getStatusCode() != 200) {
			logger.warn("Elastic transport meta parsing failed, connect directly\n\t" + rep.getStatusLine().getReasonPhrase());
			return uriAddrs(uri);
		}
		Map<String, Object> info;
		try {
			info = JsonSerder.JSON_MAPPER.der(new InputStreamReader(rep.getEntity().getContent()), Map.class);
		} catch (UnsupportedOperationException | IOException e) {
			logger.warn("Elastic transport meta parsing failed, connect directly", e);
			return uriAddrs(uri);
		}
		return ((Map<String, Object>) info.get("nodes")).entrySet().stream().map(n -> {
			Map<String, Object> node = (Map<String, Object>) n.getValue();
			// "transport_address" : "inet[/172.18.58.139:9303]"
			String transport = (String) node.get("transport_address");
			logger.debug("Elastic ntransport node [" + (String) node.get("name") + "]: " + transport + "");
			Matcher m = NODE_TRANSPORT_ADDRESS.matcher(transport);
			InetSocketAddress addr = new InetSocketAddress(m.group(1), Integer.parseInt(m.group(2)));
			return new InetSocketTransportAddress(addr);
		}).toArray(i -> new InetSocketTransportAddress[i]);
	}

	private static InetSocketTransportAddress[] uriAddrs(URISpec uri) {
		return Arrays.stream(uri.getInetAddrs()).map(InetSocketTransportAddress::new).toArray(i -> new InetSocketTransportAddress[i]);
	}

	public ElasticConnection(String connection, Map<String, String> props) throws IOException {
		this(new URISpec(connection), props);
	}

	public ElasticConnection(String url) throws IOException {
		this(url, (Map<String, String>) null);
	}

	public ElasticConnection(URISpec esURI) throws IOException {
		this(esURI, null);
	}

	public String getDefaultIndex() {
		return uri.getPathAt(0);
	}

	public String getDefaultType() {
		return uri.getFile();
	}

	@Override
	public void close() {
		try {
			super.close();
		} catch (IOException e) {
			logger.error("Close failure", e);
		}
		client().close();
	}

	public void mapping(Map<String, ?> mapping, String... types) {
		logger.debug("Mapping constructing: " + mapping);
		PutMappingRequest req = new PutMappingRequest(getDefaultIndex());
		req.source(mapping);
		Set<String> tps = new HashSet<>(Arrays.asList(types));
		if (null != getDefaultType()) tps.add(getDefaultType());
		for (String t : tps)
			if (!client().admin().indices().putMapping(req.type(t)).actionGet().isAcknowledged()) logger.error("Mapping failed" + req
					.toString());
	}
}
