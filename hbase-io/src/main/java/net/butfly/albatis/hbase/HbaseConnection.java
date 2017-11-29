package net.butfly.albatis.hbase;

import static net.butfly.albacore.paral.Sdream.of;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.ipc.RemoteWithExtrasException;
import org.apache.hadoop.hbase.util.Bytes;

import com.hzcominfo.albatis.nosql.NoSqlConnection;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.Message;

public class HbaseConnection extends NoSqlConnection<org.apache.hadoop.hbase.client.Connection> {
	private final static Logger logger = Logger.getLogger(HbaseConnection.class);
	private static final int MAX_RETRIES = 5;
	private static final int CACHED_SCAN_OBJS = 500;
	private final Map<String, Table> tables;
	private final LinkedBlockingQueue<Scan> scans = new LinkedBlockingQueue<>(CACHED_SCAN_OBJS);

public class HbaseConnection extends NoSqlConnection<Connection> {
	private static final Logger logger = Logger.getLogger(HbaseConnection.class);

	public HbaseConnection(URISpec uri) throws IOException {
		super(uri, u -> {
			Map<String, String> params = null;
			if (null != u) {
				params = new ConcurrentHashMap<>(u.getParameters());
				switch (u.getScheme()) {
				case "hbase":
					if (u.getInetAddrs().length == 1) {
						logger.warn("Deprecate master connect to hbase: " + u.getHost());
						params.put("hbase.master", "*" + u.getHost() + "*");
						break;
					}
				case "zk":
				case "zookeeper":
				case "hbase:zk":
				case "hbase:zookeeper":
					if (uri.getInetAddrs().length > 0) for (InetSocketAddress a : u.getInetAddrs()) {
						params.put(HConstants.ZOOKEEPER_QUORUM, a.getHostName());
						params.put(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.toString(a.getPort()));
					}
				}
			}
			try {
				return Hbases.connect(params);
			} catch (IOException e) {
				return null;
			}
		}, "hbase");
		tables = Maps.of();
	}

	@Override
	public void close() throws IOException {
		super.close();
		for (String k : tables.keySet())
			try {
				Table t = tables.remove(k);
				if (null != t) t.close();
			} catch (IOException e) {
				logger().error("Hbase table [" + k + "] close failure", e);
			}
		try {
			synchronized (this) {
				if (!client().isClosed()) client().close();
			}
		} catch (IOException e) {
			logger().error("Hbase close failure", e);
		}
	}

	public Message get(String table, Get get) {
		return table(table, t -> {
			Result r;
			try {
				r = t.get(get);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
	}

	public <T> T table(String name, Function<Table, T> using) {
		return using.apply(table(name));
	}

	public List<Message> get(String table, List<Get> gets) {
		if (gets == null || gets.isEmpty()) return new ArrayList<>();
		if (gets.size() == 1) return Arrays.asList(scan(table, gets.get(0)));
		return table(table, t -> {
			try {
				return of(t.get(gets)).map(r -> Hbases.Results.result(table, r)).list();
			} catch (Exception ex) {
				return of(gets).map(g -> scan(table, g)).nonNull().list();
			}
		});
	}

	@Override
	public void close() {
		try {
			super.close();
			client().close();
		} catch (IOException e) {
			logger.error("Close failure", e);
		}
	}
}
