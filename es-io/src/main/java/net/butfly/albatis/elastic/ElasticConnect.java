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

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.Message;

public interface ElasticConnect extends Connection {
	static final Logger _logger = Logger.getLogger(ElasticConnection.class);

	String getDefaultIndex();

	String getDefaultType();

	public default Message fixTable(Message m) {
		Pair<String, String> p = Elastics.dessemble(m.table());
		if (null == p.v1()) p.v1(getDefaultIndex());
		if (null == p.v2()) p.v2(getDefaultType());
		m.table(Elastics.assembly(p.v1(), p.v2()));
		return m;
	}

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
		static Map<String, Object> fetchMetadata(InetSocketAddress... rest) {
			if (_logger.isDebugEnabled()) _logger.debug("Fetch transport nodes meta from: " + Arrays.stream(rest).map(a -> a.toString())
					.collect(Collectors.joining(",")));
			try (RestClient client = RestClient.builder(Arrays.stream(rest).map(a -> new HttpHost(a.getHostName(), a.getPort())).toArray(
					i -> new HttpHost[i])).build();) {
				Response rep;
				rep = client.performRequest("GET", "_nodes");
				if (rep.getStatusLine().getStatusCode() != 200) throw new IOException(
						"Elastic transport meta parsing failed, connect directly\n\t" + rep.getStatusLine().getReasonPhrase());
				return JsonSerder.JSON_MAPPER.der(new InputStreamReader(rep.getEntity().getContent()), Map.class);
			} catch (UnsupportedOperationException | IOException e) {
				_logger.warn("Elastic transport meta parsing failed, connect directly", e);
				return null;
			}
		}

		private static String checkClusterName(String clusterFromUri, String clusterFromMeta) {
			if (null != clusterFromUri) {
				if (null != clusterFromMeta && !clusterFromMeta.equals(clusterFromUri)) _logger.warn("Elastic cluster name setting in uri ["
						+ clusterFromUri + "] conflicts to detected [" + clusterFromMeta + "], use set value [" + clusterFromUri + "].");
				return clusterFromUri;
			} else if (null != clusterFromMeta) {
				_logger.warn("Elastic cluster name not setting, use detected [" + clusterFromMeta + "].");
				return clusterFromMeta;
			} else {
				_logger.warn("Elastic cluster name not setting and detected, ignored.");
				return null;
			}
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
				_logger.trace("Elastic node detected: [" + host + ":" + rest + ":" + trans + "]");
				return new Node(host, rest, trans);
			}).toArray(i -> new Node[i]);
		}
	}

	static final class Builder extends Utils {
		private Builder() {}

		static TransportClient buildTransportClient(URISpec uri, Map<String, String> props) {
			String rest = uri.fetchParameter("rest");
			Settings.Builder settings = Settings.builder();
			settings.put(uri.getParameters("script", "batch"));
			if (null != props && !props.isEmpty()) settings.put(props);

			Map<String, Object> meta = null == rest ? null : Parser.fetchMetadata(parseRestAddrs(rest, uri.getInetAddrs()));
			String cn = Parser.checkClusterName(uri.getUsername(), null == meta ? null : (String) meta.get("cluster_name"));
			if (null == cn) settings.put("client.transport.ignore_cluster_name", true);
			else settings.put("cluster.name", cn);

			TransportClient tc = new PreBuiltTransportClient(settings.build());

			InetSocketTransportAddress[] addrs = meta == null ? Arrays.stream(uri.getInetAddrs()).map(InetSocketTransportAddress::new)
					.toArray(i -> new InetSocketTransportAddress[i])
					: Arrays.stream(Parser.getNodes(meta)).map(n -> n.transport).toArray(i -> new InetSocketTransportAddress[i]);
			_logger.debug(() -> "Elastic transport client consrtuct, cluster: [" + cn + "]\n\t" + Arrays.stream(addrs).map(a -> a
					.toString()).collect(Collectors.joining(",")));

			tc.addTransportAddresses(addrs);
			return tc;
		}

		private static InetSocketAddress[] parseRestAddrs(String rest, InetSocketAddress... inetSocketAddresses) {
			String[] hp = rest.split(":", 2);
			if (hp.length == 2) return new InetSocketAddress[] { new InetSocketAddress(hp[0], Integer.parseInt(hp[1])) };
			int port = 39200;
			try {
				port = Integer.parseInt(hp[0]);
				int p = port;
				hp = Arrays.stream(inetSocketAddresses).map(a -> new InetSocketAddress(a.getAddress(), p).toString()).toArray(
						i -> new String[i]);
			} catch (NumberFormatException e) {
				hp = Arrays.copyOfRange(hp, 0, 1);
			}
			int p = port;
			return Arrays.stream(hp).map(h -> new InetSocketAddress(h, p)).toArray(i -> new InetSocketAddress[i]);
		}

		static RestClient buildRestClient(URISpec u) {
			Map<String, Object> meta = Parser.fetchMetadata(u.getInetAddrs());
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
}
