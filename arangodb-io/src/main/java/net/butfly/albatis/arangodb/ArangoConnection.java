package net.butfly.albatis.arangodb;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

import com.arangodb.ArangoDBAsync;
import com.arangodb.ArangoDBAsync.Builder;
import com.arangodb.ArangoDatabaseAsync;
import com.arangodb.entity.BaseDocument;
import com.hzcominfo.albatis.nosql.NoSqlConnection;

import net.butfly.albacore.exception.NotImplementedException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Colls;

public class ArangoConnection extends NoSqlConnection<ArangoDBAsync> {
	private static final int MAX_CONNECTIONS = Integer.parseInt(Configs.gets("albatis.arango.connection.max.conn", "10000"));
	private static final int TIMEOUT_SECS = Integer.parseInt(Configs.gets("albatis.arango.connection.timeout", "0"));
	private static final int CHUNK_SIZE = Integer.parseInt(Configs.gets("albatis.arango.connection.chunk.size", Integer.toString(5 * 1024
			* 1024)));

	public final ArangoDatabaseAsync db;
	public final String[] tables;

	public ArangoConnection(URISpec uri) throws IOException {
		super(uri,

				u -> {

					Builder b = new ArangoDBAsync.Builder();
					for (InetSocketAddress h : u.getInetAddrs())
						b.host(h.getHostName(), h.getPort());
					if (null != u.getUsername()) b.user(u.getUsername());
					if (null != u.getPassword()) b.password(u.getPassword());
					return b.maxConnections(MAX_CONNECTIONS).chunksize(CHUNK_SIZE).build();
				}, 8529, "arango", "arangodb");
		if (uri.getPaths().length > 0) {
			this.db = client().db(uri.getPaths()[0]);
			this.tables = null != uri.getFile() ? Arrays.stream(uri.getFile().split(",")).filter(t -> !t.isEmpty()).toArray(
					i -> new String[i]) : new String[0];
		} else if (null != uri.getFile()) {
			this.db = client().db(uri.getFile());
			this.tables = new String[0];
		} else {
			this.db = null;
			this.tables = new String[0];
		}
	}

	@Override
	public void close() throws IOException {
		client().shutdown();
	}

	@Override
	public ArangoInput input(String... table) throws IOException {
		throw new NotImplementedException();
	}

	@SuppressWarnings("unchecked")
	@Override
	public ArangoOutput output() throws IOException {
		return new ArangoOutput("ArangoOutput", this);
	}

	public static class Driver implements com.hzcominfo.albatis.nosql.Connection.Driver<ArangoConnection> {
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

	public String parseAqlAsBindParams(Map<String, Object> v) {
		return "{" + v.keySet().stream().map(

				k -> k + ": @" + k).collect(Collectors.joining(", ")) + "}";

	}

	public long sizeOf(BaseDocument b) {
		return 0;
	}

	public static <T> T get(CompletableFuture<T> f) {
		if (null == f) return null;
		try {
			return TIMEOUT_SECS > 0 ? f.get(TIMEOUT_SECS, TimeUnit.SECONDS) : f.get();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (ExecutionException e) {
			Throwable ee = e.getCause();
			ee.printStackTrace();// TODO
			throw ee instanceof RuntimeException ? (RuntimeException) ee : new RuntimeException(ee);
		} catch (TimeoutException e) {
			logger.error("aql timeout for " + TIMEOUT_SECS + " seconds");
			return null;
		}
	}

	@SafeVarargs
	public static List<BaseDocument> merge(List<BaseDocument>... c) {
		List<BaseDocument> ll = Colls.list();
		for (List<BaseDocument> l : c)
			if (null != l && !l.isEmpty()) ll.addAll(l);
		return ll.isEmpty() ? null : ll;
	}

	public static CompletableFuture<List<BaseDocument>> merge(List<CompletableFuture<List<BaseDocument>>> fs) {
		if (null == fs || fs.isEmpty()) return empty();
		CompletableFuture<List<BaseDocument>> f = fs.get(0);
		for (int i = 1; i < fs.size(); i++)
			f = f.thenCombine(fs.get(i), ArangoConnection::merge);
		return f;
	}

	public static CompletableFuture<List<BaseDocument>> merge(List<BaseDocument> l, CompletableFuture<List<BaseDocument>> f) {
		return CompletableFuture.completedFuture(l).thenCombine(f, ArangoConnection::merge);
	}

	public static CompletableFuture<List<BaseDocument>> empty() {
		return CompletableFuture.completedFuture(Colls.list());
	}

	public static BinaryOperator<CompletableFuture<List<BaseDocument>>> REDUCING = (f1, f2) -> {
		if (null == f1) return f2;
		if (null == f2) return f1;
		return f1.thenCombine(f2, ArangoConnection::merge);
	};
}
