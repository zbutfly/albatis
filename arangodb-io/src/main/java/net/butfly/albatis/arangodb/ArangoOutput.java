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

import net.butfly.albacore.paral.Exeter;
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

	AtomicInteger c = new AtomicInteger();

	@Override
	protected void enqueue0(Sdream<EdgeMessage> edges) {
		CompletableFuture<List<BaseDocument>> ff = edges.map(e -> {
			CompletableFuture<List<BaseDocument>> f = null;
			Message v;
			if (null != (v = e.start())) f = async(v);
			if (null != (v = e.end())) f = null == f ? async(v) : f.thenCombineAsync(async(v), ArangoConnection::merge, Exeter.of());
			f = f.thenComposeAsync(l -> merge(l, async(e)), Exeter.of());
			f = e.edges.isEmpty() ? f
					: f.thenComposeAsync(l -> merge(l, e.edges.stream().map(ee -> async(ee)).filter(t -> null != t).reduce(REDUCING).orElse(
							ArangoConnection.empty())), Exeter.of());
			return e.nested.isEmpty() ? f : f.thenComposeAsync(l -> merge(l, async(e.nested)), Exeter.of());
		}).reduce(REDUCING);
		if (null != ff) {
			List<BaseDocument> l = get(ff);
			if (null == l || l.isEmpty()) return;
			logger().trace("Sencondary edges total: " + c.addAndGet(l.size()) + ", this: " + l.size());
			s().stats(l);
		}
	}

	private CompletableFuture<List<BaseDocument>> async(List<NestedAqls> nested) {
		if (nested.isEmpty()) return null;
		logger().trace("Nested stage of [" + nested.size() + "] aql...................... ");
		List<CompletableFuture<List<BaseDocument>>> fs = Colls.list();
		for (NestedAqls n : nested) {
			CompletableFuture<List<BaseDocument>> f = conn.db.query(n.aql, null, null, BaseDocument.class).thenApply(
					ArangoCursorAsync<BaseDocument>::asListRemaining);
			if (null != n.andThens) f = f.thenCompose(docs -> {
				List<CompletableFuture<List<BaseDocument>>> fs2 = Colls.list();
				for (BaseDocument d : docs) {
					List<NestedAqls> nexts = n.andThens.apply(d.getProperties());
					if (!nexts.isEmpty()) {
						CompletableFuture<List<BaseDocument>> f2 = async(n.andThens.apply(d.getProperties()));
						if (null != f2) fs2.add(f2);
					}
				}
				return fs2.isEmpty() ? CompletableFuture.completedFuture(docs) : merge(docs, merge(fs2));
			});
			fs.add(f);
		}
		return merge(fs);
	}

	private static final Format AQL = new MessageFormat("upsert '{'_key: @_key} insert {1} update {1} in {0} return {2}"); //

	private CompletableFuture<List<BaseDocument>> async(Message v) {
		if (null == v.key() || null == v.table() || v.isEmpty()) return ArangoConnection.empty();
		Map<String, Object> d = v.map();
		d.put("_key", v.key());
		String aql = AQL.format(new String[] { v.table(), conn.parseAqlAsBindParams(d), returnExpr });
		long t = System.currentTimeMillis();
		logger().trace("Top aql[" + t + "]: " + aql + "\n\twith params: " + d);
		return conn.db.query(aql, d, null, BaseDocument.class).thenApplyAsync(c -> {
			logger().trace("Top aql[" + t + "] spent: [" + (System.currentTimeMillis() - t) + " ms].");
			return c.asListRemaining();
		}, Exeter.of());
	}

	@Override
	public Statistic trace() {
		return new Statistic(this).<BaseDocument> sizing(b -> conn.sizeOf(b)) //
				.<BaseDocument> sampling(BaseDocument::toString);
	}
}
