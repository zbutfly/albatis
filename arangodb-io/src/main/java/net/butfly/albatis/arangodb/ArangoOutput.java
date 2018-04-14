package net.butfly.albatis.arangodb;

import static net.butfly.albatis.arangodb.ArangoConnection.REDUCING;
import static net.butfly.albatis.arangodb.ArangoConnection.get;
import static net.butfly.albatis.arangodb.ArangoConnection.merge;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.Format;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
		List<CompletableFuture<List<BaseDocument>>> fs = Colls.list();
		edges.eachs(e -> {
			if (!e.edges.isEmpty())//
				logger().trace("Nested!");
			CompletableFuture<List<BaseDocument>> f = null;
			Message v;
			if (null != (v = e.start())) f = async(v);
			if (null != (v = e.end())) f = null == f ? async(v) : f.thenCombineAsync(async(v), ArangoConnection::merge, Exeter.of());
			f = f.thenComposeAsync(l -> merge(l, async(e)), Exeter.of());
			f = e.edges.isEmpty() ? f
					: f.thenComposeAsync(l -> merge(l, e.edges.stream().map(ee -> async(ee)).filter(t -> null != t).reduce(REDUCING).orElse(
							ArangoConnection.empty())), Exeter.of());
			f = e.nested.isEmpty() ? f : f.thenComposeAsync(l -> merge(l, async(e.nested)), Exeter.of());
			if (null != f) fs.add(f);
		});
		if (fs.isEmpty()) return;
		CompletableFuture<List<BaseDocument>> ff = merge(fs);
		if (null != ff) s().stats(get(ff));
	}

	private CompletableFuture<List<BaseDocument>> async(List<NestedAqls> nested) {
		if (nested.isEmpty()) return null;
		List<CompletableFuture<List<BaseDocument>>> fs = Colls.list();
		for (NestedAqls n : nested) {
			CompletableFuture<List<BaseDocument>> f = exec(n.aql, null);
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
		return exec(aql, d);
	}

	private static final Format AVG = new DecimalFormat("0.00");
	private final AtomicLong count = new AtomicLong(), spent = new AtomicLong();

	private CompletableFuture<List<BaseDocument>> exec(String aql, Map<String, Object> param) {
		long t = System.currentTimeMillis();
		return conn.db.query(aql, param, null, BaseDocument.class).thenApplyAsync(c -> {
			long tt = System.currentTimeMillis() - t;
			logger().trace(() -> {
				long total = count.incrementAndGet();
				String avg = AVG.format(((double) spent.addAndGet(tt)) / total);
				return "AQL: [spent " + tt + " ms, total " + total + ", avg " + avg + " ms] with aql: " + aql //
						+ (null == param || param.isEmpty() ? "" : "\n\tparams: " + param) + ".";
			});
			return c.asListRemaining();
		}, Exeter.of());
	}

	@Override
	public Statistic trace() {
		return new Statistic(this).<BaseDocument> sizing(b -> conn.sizeOf(b)) //
				.<BaseDocument> sampling(BaseDocument::toString);
	}
}
