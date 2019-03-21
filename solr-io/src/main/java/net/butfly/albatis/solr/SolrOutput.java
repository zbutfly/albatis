package net.butfly.albatis.solr;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServerException;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.Rmap.Op;

public final class SolrOutput extends OutputBase<Rmap> {
	private static final long serialVersionUID = 4875270142242208468L;
	public static final @SolrProps String MAX_CONCURRENT_OP_PROP_NAME = SolrProps.OUTPUT_CONCURRENT_OPS;
	public static final int MAX_CONCURRENT_OP_DEFAULT = 100;
	public static final int SUGGEST_BATCH_SIZE = 200;
	private static final int DEFAULT_AUTO_COMMIT_MS = 30000;
	private final SolrConnection conn;
	private final String keyFieldName;

	public SolrOutput(String name, SolrConnection conn) throws IOException {
		this(name, conn, "id");
	}

	public SolrOutput(String name, SolrConnection conn, String keyFieldName) throws IOException {
		super(name);
		this.conn = conn;
		this.keyFieldName = keyFieldName;
	}

	@Override
	public URISpec target() {
		return conn.uri();
	}

	@Override
	public void close() {
		super.close();
		try {
			for (String coll : conn.getColls())
				conn.client.commit(coll, false, false);
		} catch (IOException | SolrServerException e) {
			logger().error("Close failure", e);
		}
		try {
			conn.close();
		} catch (IOException e) {
			logger().error("Close failure", e);
		}
	}

	@Override
	protected void enqsafe(Sdream<Rmap> msgs) {
		Map<String, Map<Integer, List<Rmap>>> map = Maps.of();
		msgs.eachs(m -> map.compute(m.table().name, (coll, colls) -> {
			if (null == colls) colls = Maps.of();
			colls.compute(m.op(), (op, ops) -> {
				if (null == ops) ops = Colls.list();
				ops.add(m);
				return ops;
			});
			return colls;
		}));
		for (String coll : map.keySet())
			for (int op : map.get(coll).keySet()) {
				List<Rmap> ms = map.get(coll).get(op);
				try {
					switch (op) {
					case Op.DELETE:
						conn.client.deleteById(coll, Sdream.of(map.get(coll).get(op))//
								.map(t -> null == t.key() ? null : t.key().toString()).list(), DEFAULT_AUTO_COMMIT_MS);
						break;
					default:
						conn.client.add(coll, Sdream.of(map.get(coll).get(op)).map(m -> Solrs.input(m, keyFieldName)).list(),
								DEFAULT_AUTO_COMMIT_MS);
					}
					succeeded(ms.size());
				} catch (Exception ex) {
					failed(Sdream.of(ms));
				}
			}
	}

	@Override
	public void commit() {
		try {
			conn.client.commit(false, false, true);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
