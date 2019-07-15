package net.butfly.albatis.arangodb;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.DecimalFormat;
import java.text.Format;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import com.arangodb.ArangoCursorAsync;
import com.arangodb.ArangoDBAsync;
import com.arangodb.ArangoDBAsync.Builder;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabaseAsync;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.LoadBalancingStrategy;
import com.arangodb.model.AqlQueryOptions;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.BinaryOperator;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.TableDesc;

public class ArangoConnection extends DataConnection<ArangoDBAsync> {
	private static final Logger logger = Logger.getLogger(ArangoConnection.class);
	private static final int MAX_CONNECTIONS = Integer.parseInt(Configs.gets("albatis.arango.connection.max.conn", "0"));
	private static final int TIMEOUT_SECS = Integer.parseInt(Configs.gets("albatis.arango.connection.timeout", "0"));
	private static final int CHUNK_SIZE = Integer.parseInt(Configs.gets("albatis.arango.connection.chunk.size", //
			Integer.toString(5 * 1024 * 1024)));

	public final ArangoDatabaseAsync db;
	public final String[] tables;

	public ArangoConnection(URISpec uri) throws IOException {
		super(uri, 8529, "arango", "arangodb");
		if (uri.getPaths().length > 0) {
			this.db = client.db(uri.getPaths()[0]);
			this.tables = null != uri.getFile() ? Arrays.stream(uri.getFile().split(",")).filter(t -> !t.isEmpty()).toArray(i -> new String[i])
					: new String[0];
		} else if (null != uri.getFile()) {
			this.db = client.db(uri.getFile());
			this.tables = new String[0];
		} else {
			this.db = null;
			this.tables = new String[0];
		}
	}

	@Override
	protected ArangoDBAsync initialize(URISpec uri) {
		Builder b = new ArangoDBAsync.Builder();
		for (InetSocketAddress h : uri.getInetAddrs()) b.host(h.getHostName(), h.getPort());
		if (null != uri.getUsername()) b.user(uri.getUsername());
		if (null != uri.getPassword()) b.password(uri.getPassword());
		if (MAX_CONNECTIONS > 0) b.maxConnections(MAX_CONNECTIONS);
		else if (uri.getInetAddrs().length > 1) b.maxConnections(uri.getInetAddrs().length);
		else b.maxConnections(8);
		if (uri.getInetAddrs().length > 1) {
			b.loadBalancingStrategy(LoadBalancingStrategy.ROUND_ROBIN);
			b.acquireHostList(true);
		}
		return b.chunksize(CHUNK_SIZE).build();
	}

	@Override
	public void close() throws IOException {
		client.shutdown();
	}

	@SuppressWarnings("unchecked")
	@Override
	public ArangoInput inputRaw(TableDesc... table) throws IOException {
		ArangoInput i = new ArangoInput("ArangoInput", this);
		return i;
	}

	@SuppressWarnings("unchecked")
	@Override
	public ArangoOutput outputRaw(TableDesc... table) throws IOException {
		return new ArangoOutput("ArangoOutput", this);
	}

	public static class Driver implements net.butfly.albatis.Connection.Driver<ArangoConnection> {
		static {
			DriverManager.register(new Driver());
		}

		@Override
		public ArangoConnection connect(URISpec uriSpec) throws IOException {
			return new ArangoConnection(uriSpec);
		}

		@Override
		public List<String> schemas() {
			return Colls.list("arangodb", "arango");
		}
	}

	public long sizeOf(BaseDocument b) {
		return 0;
	}

	private static final Format AVG = new DecimalFormat("0.00");
	private final AtomicLong count = new AtomicLong(), spent = new AtomicLong();

	public CompletableFuture<List<BaseDocument>> exec(String aql, Map<String, Object> param) {
		long t = System.currentTimeMillis();
		return db.query(aql, param, null, BaseDocument.class).thenApplyAsync(c -> {
			long tt = System.currentTimeMillis() - t;
			logger().trace(() -> {
				long total = count.incrementAndGet();
				String avg = AVG.format(((double) spent.addAndGet(tt)) / total);
				return "AQL: [spent " + tt + " ms, total " + total + ", avg " + avg + " ms] with aql: " + aql //
						+ (Colls.empty(param) ? "" : "\n\tparams: " + param) + ".";
			});
			return c.asListRemaining();
		}, Exeter.of());
	}

	public static <T> T get(CompletableFuture<T> f) {
		if (null == f) return null;
		try {
			return TIMEOUT_SECS > 0 ? f.get(TIMEOUT_SECS, TimeUnit.SECONDS) : f.get();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (ExecutionException e) {
			Throwable ee = e.getCause();
			if (ee instanceof ArangoDBException)
				// ArangoDBException ae = (ArangoDBException) ee;
				// if (503 == ae.getResponseCode() && 21003 == ae.getErrorNum())
				// logger.error("fail: " + ae.getMessage());
				return null;
			logger.error("fail", ee);
			throw ee instanceof RuntimeException ? (RuntimeException) ee : new RuntimeException(ee);
		} catch (TimeoutException e) {
			// logger.error("aql timeout for " + TIMEOUT_SECS + " seconds");
			return null;
		}
	}

	@SafeVarargs
	public static List<BaseDocument> merge(List<BaseDocument>... c) {
		List<BaseDocument> ll = Colls.list();
		for (List<BaseDocument> l : c) if (null != l && !l.isEmpty()) ll.addAll(l);
		return ll.isEmpty() ? null : ll;
	}

	public static CompletableFuture<List<BaseDocument>> merge(List<CompletableFuture<List<BaseDocument>>> fs) {
		if (Colls.empty(fs)) return empty();
		CompletableFuture<List<BaseDocument>> f = fs.get(0);
		for (int i = 1; i < fs.size(); i++) f = f.thenCombineAsync(fs.get(i), ArangoConnection::merge, Exeter.of());
		return f;
	}

	public static CompletableFuture<List<BaseDocument>> empty() {
		return CompletableFuture.completedFuture(Colls.list());
	}

	public static BinaryOperator<CompletableFuture<List<BaseDocument>>> REDUCING = (f1, f2) -> {
		if (null == f1) return f2;
		if (null == f2) return f1;
		return f1.thenCombineAsync(f2, ArangoConnection::merge, Exeter.of());
	};

	public boolean collectionExists(String collectionName) {
		return db.collection(collectionName).exists().isDone();
	}

	public CompletableFuture<ArangoCursorAsync<BaseDocument>> cursor(String aql) {
		AqlQueryOptions opts = new AqlQueryOptions();
		opts.ttl(Integer.MAX_VALUE);
		opts.stream(true);
		return db.query(aql, null, opts, BaseDocument.class);
	}
}
