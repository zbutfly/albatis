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

	@SafeVarargs
	private static List<BaseDocument> arangoStats(List<BaseDocument>... c) {
		List<BaseDocument> ll = Colls.list();
		for (List<BaseDocument> l : c)
			if (null != l && !l.isEmpty()) ll.addAll(l);
		return ll.isEmpty() ? null : ll;
	}

	@Override
	protected void enqueue0(Sdream<EdgeMessage> edges) {
		CompletableFuture<List<BaseDocument>> ff = edges.map(e -> edge((EdgeMessage) e).thenApply(l -> {
			if (null != e.then) e.then.run();
			return l;
		})).reduce((f1, f2) -> {
			if (null == f1) return f2;
			if (null == f2) return f1;
			return f1.thenCombine(f2, ArangoOutput::arangoStats);
		});
		if (null != ff) s().stats(get(ff));
	}

	private CompletableFuture<List<BaseDocument>> edge(EdgeMessage e) {
		CompletableFuture<List<BaseDocument>> f = null;
		Message v;
		if (null != (v = ((EdgeMessage) e).start())) f = async(v);
		if (null != (v = ((EdgeMessage) e).end())) f = null == f ? async(v) : f.thenCombine(async(v), ArangoOutput::arangoStats);
		return f.thenCompose(l -> {
			AtomicReference<CompletableFuture<List<BaseDocument>>> ref = new AtomicReference<>(async(e));
			((EdgeMessage) e).edges.forEach(ee -> ref.set(ref.get().thenCombine(async(ee), (t, u) -> arangoStats(t, u, l))));
			return ref.get();
		});
	}

	private static final Format AQL = new MessageFormat("upsert '{'_key: @_key} insert {1} update '{'} in {0} return {2}"); //

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
