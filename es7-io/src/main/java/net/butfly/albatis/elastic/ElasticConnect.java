package net.butfly.albatis.elastic;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.Connection;
import net.butfly.albatis.ddl.vals.GeoPointVal;
import net.butfly.albatis.io.Rmap;
import org.apache.http.HttpHost;
import org.elasticsearch.client.*;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import static net.butfly.albacore.paral.Sdream.of;

public interface ElasticConnect extends Connection {
	static final Logger LOGGER = Logger.getLogger(ElasticConnection.class);

	String getDefaultIndex();

	String getDefaultType();

	public default Rmap fixTable(Rmap m) {
		Object v;
		for (String k : m.keySet())
			if ((v = m.get(k)) instanceof GeoPointVal) m.put(k, v.toString());
		return m;
	}

	@SuppressWarnings("unchecked")
	static <C extends ElasticConnect> C connect(URISpec uri) throws IOException {
		switch (uri.getScheme().toLowerCase()) {
		case "es:rest":
		case "elasticsearch:rest":
			return (C) new ElasticRestHighLevelConnection(uri);
		case "es":
		case "elasticsearch":
		case "es:transport":
		case "elasticsearch:transport":
			return (C) new ElasticConnection(uri);
		default:
			throw new IllegalArgumentException("schema not supported: " + uri.getSchema());
		}
	}

	static final class Parser extends Utils {
		private Parser() {}

		@SuppressWarnings({ "unchecked" })
		static Map<String, Object> fetchMetadata(InetSocketAddress... rest) {
			if (LOGGER.isDebugEnabled()) LOGGER.debug("Fetch transport nodes meta from: " + of(rest).joinAsString(InetSocketAddress::toString,
					","));
			HttpHost[] v = Arrays.stream(rest).map(a -> new HttpHost(a.getHostName(), a.getPort())).toArray(i -> new HttpHost[i]);
			try (RestClient client = RestClient.builder(v).build()) {
				Response rep;
				rep = client.performRequest(new Request("GET","_nodes"));
				if (rep.getStatusLine().getStatusCode() != 200) //
					throw new IOException("Elastic transport meta parsing failed, connect directly\n\t" //
							+ rep.getStatusLine().getReasonPhrase());
				return JsonSerder.JSON_MAPPER.der(new InputStreamReader(rep.getEntity().getContent()), Map.class);
			} catch (UnsupportedOperationException | IOException e) {
				LOGGER.warn("Elastic transport meta parsing failed, connect directly", e);
				return null;
			}
		}

		@SuppressWarnings("unchecked")
		static Map<String, Object> fetchMetadata(HttpClientConfigCallback callback, InetSocketAddress... rest) {
			if (LOGGER.isDebugEnabled()) LOGGER.debug("Fetch transport nodes meta from: " + of(rest).joinAsString(InetSocketAddress::toString,
					","));
			HttpHost[] v = Arrays.stream(rest).map(a -> new HttpHost(a.getHostName(), a.getPort())).toArray(i -> new HttpHost[i]);
			try (RestClient client = RestClient.builder(v).setHttpClientConfigCallback(callback).build()) {
				Response rep;
				rep = client.performRequest(new Request("GET","_nodes"));
				if (rep.getStatusLine().getStatusCode() != 200) //
					throw new IOException("Elastic transport meta parsing failed, connect directly\n\t" //
							+ rep.getStatusLine().getReasonPhrase());
				return JsonSerder.JSON_MAPPER.der(new InputStreamReader(rep.getEntity().getContent()), Map.class);
			} catch (UnsupportedOperationException | IOException e) {
				LOGGER.warn("Elastic transport meta parsing failed, connect directly", e);
				return null;
			}
		}

		private static String checkClusterName(String clusterFromUri, String clusterFromMeta) {
			if (null != clusterFromUri) {
				if (null != clusterFromMeta && !clusterFromMeta.equals(clusterFromUri)) LOGGER.warn("Elastic cluster name setting in uri ["
						+ clusterFromUri + "] conflicts to detected [" + clusterFromMeta + "], use set value [" + clusterFromUri + "].");
				return clusterFromUri;
			} else if (null != clusterFromMeta) {
				LOGGER.warn("Elastic cluster name not setting, use detected [" + clusterFromMeta + "].");
				return clusterFromMeta;
			} else {
				LOGGER.warn("Elastic cluster name not setting and detected, ignored.");
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
				int trans = Integer.parseInt((String) ((Map<String, Object>) ((Map<String, Object>) settings.get("transport")).get("tcp")).get(
						"port"));
				LOGGER.trace("Elastic node detected: [" + host + ":" + rest + ":" + trans + "]");
				return new Node(host, rest, trans);
			}).toArray(i -> new Node[i]);
		}
	}

	static final class Builder extends Utils {
		private Builder() {}

		static TransportClient buildTransportClient(URISpec uri, Map<String, String> props) {
			kerberosAuth();
			String rest = uri.fetchParameter("rest");
			Settings.Builder settings = Settings.builder();
			uri.getParameters("script", "batch", "df").forEach(settings::put);
			if (null != props && !props.isEmpty()) props.forEach(settings::put);

			Map<String, Object> meta = null == rest ? null : Parser.fetchMetadata(parseRestAddrs(rest, uri.getInetAddrs()));
			String cn = Parser.checkClusterName(uri.getUsername(), null == meta ? null : (String) meta.get("cluster_name"));
			if (null == cn) {
				settings.put("client.transport.ignore_cluster_name", true);
				LOGGER.info("ElasticSearch \"cluster.name\" not found, enable ignore setting.");
			} else {
				settings.put("cluster.name", cn);
				LOGGER.info("ElasticSearch \"cluster.name\" set to: " + cn);
			}
			TransportAddress[] addrs = meta == null ? //
					Arrays.stream(uri.getInetAddrs()).map(TransportAddress::new).toArray(i -> new TransportAddress[i])
					: Arrays.stream(Parser.getNodes(meta)).map(n -> n.transport).toArray(i -> new TransportAddress[i]);
			LOGGER.debug(() -> "Elastic transport client construct, cluster: [" + cn + "]\n\t" + of(addrs).joinAsString(
					TransportAddress::toString, ","));

			TransportClient tc = new PreBuiltTransportClient(settings.build());
			tc.addTransportAddresses(addrs);
			return tc;
		}

		private static void kerberosAuth() {
			Properties p = new Properties();
			try {
				InputStream is = ElasticConnect.class.getResourceAsStream("/albatis-es.properties");
				if (null != is) p.load(is);
				else return;
			} catch (IOException e) {
				logger.error("Loading property file is failure", e);
			}
			logger.info("start normal kerberos setting!");
			Map<String, String> props = Maps.of(p);
			for (Map.Entry<String, String> c : props.entrySet()) System.setProperty(c.getKey(), c.getValue());
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

		static RestHighLevelClient buildRestHighLevelClient(URISpec u) {
			Map<String, Object> meta = Parser.fetchMetadata(u.getInetAddrs());
			HttpHost[] addrs;
			if (meta == null) addrs = Arrays.stream(u.getInetAddrs()).map(a -> new HttpHost(a.getHostName(), a.getPort())).toArray(
					i -> new HttpHost[i]);
			else addrs = Arrays.stream(Parser.getNodes(meta)).map(n -> n.rest).toArray(i -> new HttpHost[i]);
			return new RestHighLevelClient(RestClient.builder(addrs));
		}

		static RestHighLevelClient buildRestHighLevelClientWithKerberos(URISpec u) {
			RestClientBuilder.HttpClientConfigCallback httpClientConfigCallback = new CustomHttpClientConfigCallbackHandler(u.getUsername(), null);
			Map<String, Object> meta = Parser.fetchMetadata(httpClientConfigCallback, u.getInetAddrs());
			HttpHost[] addrs;
			if (meta == null) addrs = Arrays.stream(u.getInetAddrs()).map(a -> new HttpHost(a.getHostName(), a.getPort())).toArray(
					i -> new HttpHost[i]);
			else addrs = Arrays.stream(Parser.getNodes(meta)).map(n -> n.rest).toArray(i -> new HttpHost[i]);
			return new RestHighLevelClient(RestClient.builder(addrs).setHttpClientConfigCallback(httpClientConfigCallback));
		}
	}

	static class Node {
		final HttpHost rest;
		final TransportAddress transport;

		Node(String host, int restPort, int transportPort) {
			super();
			this.rest = new HttpHost(host, restPort);
			this.transport = new TransportAddress(new InetSocketAddress(host, transportPort));
		}

		@Override
		public String toString() {
			return "REST: " + rest.toString() + "; Transport: " + transport.toString();
		}

		HttpHost rest() {
			return rest;
		}

		TransportAddress transport() {
			return transport;
		}
	}
}
