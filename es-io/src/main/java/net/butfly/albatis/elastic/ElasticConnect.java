package net.butfly.albatis.elastic;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.hzcominfo.albatis.nosql.Connection;

import net.butfly.albacore.io.utils.URISpec;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.logger.Logger;

public interface ElasticConnect extends Connection {
	static final Logger _logger = Logger.getLogger(ElasticConnection.class);

	String getDefaultIndex();

	String getDefaultType();

	void mapping(Map<String, Object> mapping, String... types);

	@SuppressWarnings("unchecked")
	static <C extends ElasticConnect> C connect(URISpec uri) throws IOException {
		switch (uri.getScheme().toLowerCase()) {
		case "es":
		case "elasticsearch":
		case "es:transport":
		case "elasticsearch:transport":
			return (C) new ElasticConnection(uri);
		case "http":
		case "https":
		case "es:http":
		case "elasticsearch:https":
			return (C) new ElasticRestConnection(uri);
		default:
			throw new IllegalArgumentException("schema not supported: " + uri.getScheme());
		}
	}

	static final class Parser extends Utils {
		private Parser() {}

		@SuppressWarnings("unchecked")
		static Map<String, Object> fetchMetadata(InetSocketAddress... rest) throws IOException {
			if (_logger.isDebugEnabled()) _logger.debug("Fetch transport nodes meta from: " + Arrays.stream(rest).map(a -> a.toString())
					.collect(Collectors.joining(",")));
			try (RestClient client = RestClient.builder(Arrays.stream(rest).map(a -> new HttpHost(a.getHostName(), a.getPort())).toArray(
					i -> new HttpHost[i])).build();) {
				Response rep;
				rep = client.performRequest("GET", "_nodes");
				if (rep.getStatusLine().getStatusCode() != 200) throw new IOException(
						"Elastic transport meta parsing failed, connect directly\n\t" + rep.getStatusLine().getReasonPhrase());
				Map<String, Object> info;
				try {
					info = JsonSerder.JSON_MAPPER.der(new InputStreamReader(rep.getEntity().getContent()), Map.class);
				} catch (UnsupportedOperationException e) {
					throw new IOException("Elastic transport meta parsing failed, connect directly", e);
				}
				return info;
			}
		}

		static String getClusterName(Map<String, Object> meta) {
			return (String) meta.get("cluster_name");
		}

		@SuppressWarnings("unchecked")
		static Node[] getNodes(Map<String, Object> meta) {
			return ((Map<String, Object>) meta.get("nodes")).entrySet().stream().map(n -> {
				Map<String, Object> node = (Map<String, Object>) n.getValue();
				String host = (String) node.get("host");
				Map<String, Object> settings = (Map<String, Object>) node.get("settings");
				int rest = Integer.parseInt((String) ((Map<String, Object>) settings.get("http")).get("port"));
				int trans = Integer.parseInt((String) ((Map<String, Object>) ((Map<String, Object>) settings.get("transport")).get("tcp"))
						.get("port"));
				_logger.debug("Elastic node found: [" + host + ":" + rest + ":" + trans + "]");
				return new Node(host, rest, trans);
			}).toArray(i -> new Node[i]);
		}
	}

	static final class Builder extends Utils {
		private Builder() {}

		static TransportClient buildTransportClient(URISpec u, Map<String, String> props, boolean detect) {
			Settings.Builder settings = Settings.builder();
			settings.put(u.getParameters());
			if (null != props && !props.isEmpty()) settings.put(props);
			Map<String, Object> meta = null;
			String cn = null;
			try {
				meta = Parser.fetchMetadata(u.getInetAddrs());
				cn = Parser.getClusterName(meta);
			} catch (IOException e) {
				_logger.warn("Elastic transport nodes parsing failed, connect directly", e);
			}
			if (null != u.getUsername()) {
				if (null != cn && !cn.equals(u.getUsername())) _logger.warn("Elastic cluster name setting in uri [" + u.getUsername()
						+ "] conflicts to detected [" + cn + "].");
				settings.put("cluster.name", u.getUsername());
			} else if (null != cn) {
				_logger.warn("Elastic cluster name not setting, use detected [" + cn + "].");
				settings.put("cluster.name", cn);
			} else {
				_logger.warn("Elastic cluster name not setting and detected, ignored.");
				settings.put("client.transport.ignore_cluster_name", true);
			}
			settings.remove("batch");
			settings.remove("script");
			TransportClient tc = new PreBuiltTransportClient(settings.build());
			InetSocketTransportAddress[] addrs;
			if (meta == null) addrs = Arrays.stream(u.getInetAddrs()).map(InetSocketTransportAddress::new).toArray(
					i -> new InetSocketTransportAddress[i]);
			else addrs = Arrays.stream(Parser.getNodes(meta)).map(n -> n.transport).toArray(i -> new InetSocketTransportAddress[i]);
			tc.addTransportAddresses(addrs);
			return tc;
		}

		static RestClient buildRestClient(URISpec u, Map<String, String> props) {
			Settings.Builder settings = Settings.builder();
			settings.put(u.getParameters());
			if (null != props && !props.isEmpty()) settings.put(props);
			Map<String, Object> meta = null;
			String cn = null;
			try {
				meta = Parser.fetchMetadata(u.getInetAddrs());
				cn = Parser.getClusterName(meta);
			} catch (IOException e) {
				_logger.warn("Elastic transport nodes parsing failed, connect directly", e);
			}
			if (null != u.getUsername()) {
				if (null != cn && !cn.equals(u.getUsername())) _logger.warn("Elastic cluster name setting in uri [" + u.getUsername()
						+ "] conflicts to detected [" + cn + "].");
				settings.put("cluster.name", u.getUsername());
			} else if (null != cn) {
				_logger.warn("Elastic cluster name not setting, use detected [" + cn + "].");
				settings.put("cluster.name", cn);
			} else {
				_logger.warn("Elastic cluster name not setting and detected, ignored.");
				settings.put("client.transport.ignore_cluster_name", true);
			}
			settings.remove("batch");
			settings.remove("script");

			HttpHost[] addrs;
			if (meta == null) addrs = Arrays.stream(u.getInetAddrs()).map(a -> new HttpHost(a.getHostName(), a.getPort())).toArray(
					i -> new HttpHost[i]);
			else addrs = Arrays.stream(Parser.getNodes(meta)).map(n -> n.transport).toArray(i -> new HttpHost[i]);
			return RestClient.builder(addrs).build();
		}
	}

	static class Node {
		final HttpHost rest;
		final InetSocketTransportAddress transport;

		Node(String host, int restPort, int transportPort) {
			super();
			this.rest = new HttpHost(host, restPort);
			this.transport = new InetSocketTransportAddress(new InetSocketAddress(host, transportPort));
		}

		@Override
		public String toString() {
			return "REST: " + rest.toString() + "; Transport: " + transport.toString();
		}

		HttpHost rest() {
			return rest;
		}

		InetSocketTransportAddress transport() {
			return transport;
		}
	}

	boolean update(String type, ElasticMessage es);

	long update(String type, ElasticMessage... es);
}
