package net.butfly.albatis.arangodb;

import static net.butfly.albatis.arangodb.ArangoConnection.get;

import java.io.IOException;
import java.text.Format;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import com.arangodb.ArangoCursorAsync;
import com.arangodb.entity.BaseDocument;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Statistic;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.OutputBase;
import static net.butfly.albatis.arangodb.ArangoConnection.merge;

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

	@Override
	protected void enqueue0(Sdream<EdgeMessage> edges) {
		CompletableFuture<List<BaseDocument>> ff = edges.map(e -> {
			CompletableFuture<List<BaseDocument>> f1 = edge((EdgeMessage) e);
			if (null == e.then) return f1;
			CompletableFuture<List<BaseDocument>> f11 = e.then.get();
			return f1.thenCompose(l -> merge(l, f11));
		}).reduce((f1, f2) -> {
			if (null == f1) return f2;
			if (null == f2) return f1;
			return f1.thenCombine(f2, ArangoConnection::merge);
		});
		if (null != ff) s().stats(get(ff));
	}

	private CompletableFuture<List<BaseDocument>> edge(EdgeMessage e) {
		CompletableFuture<List<BaseDocument>> f = null;
		Message v;
		if (null != (v = ((EdgeMessage) e).start())) f = async(v);
		if (null != (v = ((EdgeMessage) e).end())) f = null == f ? async(v) : f.thenCombine(async(v), ArangoConnection::merge);
		if (e.edges.isEmpty()) return f.thenCompose(l -> async(e));
		return f.thenCompose(l -> {
			AtomicReference<CompletableFuture<List<BaseDocument>>> ref = new AtomicReference<>(async(e));
			e.edges.forEach(ee -> ref.set(ref.get().thenCombine(async(ee), (t, u) -> ArangoConnection.merge(t, u, l))));
			return ref.get();
		});
	}

	private static final Format AQL = new MessageFormat("upsert '{'_key: @_key} insert {1} update {1} in {0} return {2}"); //

	private CompletableFuture<List<BaseDocument>> async(Message v) {
		if (null == v.key() || null == v.table() || v.isEmpty()) return CompletableFuture.completedFuture(Colls.list());
		Map<String, Object> d = v.map();
		d.put("_key", v.key());
		String aql = AQL.format(new String[] { v.table(), conn.parseAqlAsBindParams(d), returnExpr });
		CompletableFuture<List<BaseDocument>> f = conn.db.query(aql, d, null, BaseDocument.class).thenApply(
				ArangoCursorAsync<BaseDocument>::asListRemaining);
		return f;
	}

	@Override
	public Statistic trace() {
		return new Statistic(this).<BaseDocument> sizing(b -> conn.sizeOf(b)) //
				.<BaseDocument> sampling(BaseDocument::toString);
	}
}
