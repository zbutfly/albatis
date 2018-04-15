package net.butfly.albatis.arangodb;

import static net.butfly.albatis.arangodb.ArangoConnection.get;
import static net.butfly.albatis.arangodb.ArangoConnection.merge;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.Format;
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
import net.butfly.albatis.io.OutputBase;

public class ArangoOutput extends OutputBase<EdgeMessage> {
	private final ArangoConnection conn;

	protected ArangoOutput(String name, ArangoConnection conn) {
		super(name);
		this.conn = conn;
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
		edges.eachs(e -> fs.add(execRaw(e)));
		if (!fs.isEmpty()) get(merge(fs));
	}

	private CompletableFuture<List<BaseDocument>> execRaw(EdgeMessage e) {
		CompletableFuture<List<BaseDocument>> f = null;
		EdgeMessage v;
		if (null != (v = e.start())) f = execRaw(v);
		if (null != (v = e.end())) f = null == f ? execRaw(v) : f.thenCombineAsync(execRaw(v), ArangoConnection::merge, Exeter.of());
		f = null == f ? exec(e.aql, e.map()) : f.thenComposeAsync(l -> exec(e.aql, e.map()), Exeter.of());
		if (e.nested()) f = f.thenComposeAsync(l -> {
			List<CompletableFuture<List<BaseDocument>>> fff = Colls.list();
			for (BaseDocument d : l)
				for (EdgeMessage n : e.applyThen(d))
					fff.add(execRaw(n));
			return fff.isEmpty() ? ArangoConnection.empty() : merge(fff);
		}, Exeter.of());
		return f;
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
			return s().stats(c.asListRemaining());
		}, Exeter.of());
	}

	@Override
	public Statistic trace() {
		return new Statistic(this).<BaseDocument> sizing(b -> conn.sizeOf(b)) //
				.<BaseDocument> sampling(BaseDocument::toString);
	}
}
