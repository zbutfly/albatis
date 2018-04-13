package net.butfly.albatis.arangodb;

import static net.butfly.albatis.arangodb.ArangoConnection.REDUCING;
import static net.butfly.albatis.arangodb.ArangoConnection.get;
import static net.butfly.albatis.arangodb.ArangoConnection.merge;

import java.io.IOException;
import java.text.Format;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import com.arangodb.ArangoCursorAsync;
import com.arangodb.entity.BaseDocument;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.OutputBase;

public class ArangoOutput extends OutputBase<EdgeMessage> {
	private final ArangoConnection conn;
	private final String returnExpr;

	protected ArangoOutput(String name, ArangoConnection conn) {
		super(name);
		this.conn = conn;
		returnExpr = s().enabledMore() ? "NEW" : "{_key: NEW._key}";
	}

	@Override
	public void close() {
		try {
			conn.close();
		} catch (IOException e) {
			throw new RuntimeException();
		}
	}

	AtomicInteger c = new AtomicInteger();

	@Override
	protected void enqueue0(Sdream<EdgeMessage> edges) {
		CompletableFuture<List<BaseDocument>> ff = edges.map(e -> {
			CompletableFuture<List<BaseDocument>> f1 = edge((EdgeMessage) e);
			return null == e.then ? f1 : f1.thenCompose(l -> merge(l, e.then.get()));
		}).reduce(REDUCING);
		if (null != ff) {
			List<BaseDocument> l = get(ff);
			if (null == l || l.isEmpty()) return;
			logger().trace("Sencondary edges total: " + c.addAndGet(l.size()) + ", this: " + l.size());
			s().stats(l);
		}
	}

	private CompletableFuture<List<BaseDocument>> edge(EdgeMessage e) {
		CompletableFuture<List<BaseDocument>> f = null;
		Message v;
		if (null != (v = ((EdgeMessage) e).start())) f = async(v);
		if (null != (v = ((EdgeMessage) e).end())) f = null == f ? async(v) : f.thenCombine(async(v), ArangoConnection::merge);
		f = f.thenCompose(l -> async(e));
		return e.edges.isEmpty() ? f
				: f.thenCompose(l -> merge(l, e.edges.stream().map(ee -> async(ee)).reduce(REDUCING).orElse(ArangoConnection.empty())));
	}

	private static final Format AQL = new MessageFormat("upsert '{'_key: @_key} insert {1} update {1} in {0} return {2}"); //

	private CompletableFuture<List<BaseDocument>> async(Message v) {
		if (null == v.key() || null == v.table() || v.isEmpty()) return ArangoConnection.empty();
		Map<String, Object> d = v.map();
		d.put("_key", v.key());
		String aql = AQL.format(new String[] { v.table(), conn.parseAqlAsBindParams(d), returnExpr });
		return conn.db.query(aql, d, null, BaseDocument.class).thenApply(ArangoCursorAsync<BaseDocument>::asListRemaining);
	}

	@Override
	public Statistic trace() {
		return new Statistic(this).<BaseDocument> sizing(b -> conn.sizeOf(b)) //
				.<BaseDocument> sampling(BaseDocument::toString);
	}
}
