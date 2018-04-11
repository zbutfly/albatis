package net.butfly.albatis.arangodb;

import java.io.IOException;
import java.text.Format;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

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

	private List<BaseDocument> arangoStats(List<BaseDocument> c1, List<BaseDocument> c2) {
		if (null == c1) return c2;
		if (null == c2) return c1;
		c1.addAll(c2);
		return c1;
	}

	@Override
	protected void enqueue0(Sdream<EdgeMessage> edges) {
		CompletableFuture<List<BaseDocument>> ff = edges.map(e -> {
			Message v;
			CompletableFuture<List<BaseDocument>> f = null;
			if (null != (v = e.start())) f = async(v);
			if (null != (v = e.end())) f = null == f ? async(v) : f.thenCombine(async(v), this::arangoStats);
			return f.thenCompose(n -> {
				AtomicReference<CompletableFuture<List<BaseDocument>>> ref = new AtomicReference<>(async(e));
				e.edges.forEach(ee -> ref.set(ref.get().thenCombine(async(ee), this::arangoStats)));
				return ref.get();
			});
		}).reduce((f1, f2) -> {
			if (null == f1) return f2;
			if (null == f2) return f1;
			return f1.thenCombine(f2, this::arangoStats);
		});
		if (null != ff) try {
			s().stats(ff.get());
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (ExecutionException e) {
			Throwable ee = e.getCause();
			throw ee instanceof RuntimeException ? (RuntimeException) ee : new RuntimeException(ee);
		}
	}

	private static final Format AQL = new MessageFormat("upsert '{'_key: @_key} insert {1} update '{'} in {0} return {2}"); //

	private CompletableFuture<List<BaseDocument>> async(Message v) {
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
