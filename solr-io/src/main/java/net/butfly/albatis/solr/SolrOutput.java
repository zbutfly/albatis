package net.butfly.albatis.solr;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServerException;

import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.Message.Op;
import net.butfly.albatis.io.SafeOutput;

public final class SolrOutput extends SafeOutput<Message> {
	public static final @SolrProps String MAX_CONCURRENT_OP_PROP_NAME = SolrProps.OUTPUT_CONCURRENT_OPS;
	public static final int MAX_CONCURRENT_OP_DEFAULT = 100;
	public static final int SUGGEST_BATCH_SIZE = 200;
	private static final int DEFAULT_AUTO_COMMIT_MS = 30000;
	private final SolrConnection solr;
	private final String keyFieldName;

	public SolrOutput(String name, SolrConnection conn) throws IOException {
		this(name, conn, "id");
	}

	public SolrOutput(String name, SolrConnection conn, String keyFieldName) throws IOException {
		super(name);
		solr = conn;
		this.keyFieldName = keyFieldName;
		open();
	}

	@Override
	public void close() {
		super.close();
		try {
			for (String core : solr.getCores())
				solr.client().commit(core, false, false);
		} catch (IOException | SolrServerException e) {
			logger().error("Close failure", e);
		}
		try {
			solr.close();
		} catch (IOException e) {
			logger().error("Close failure", e);
		}
	}

	@Override
	protected void enqueue0(Sdream<Message> msgs) {
		Map<String, Map<Integer, List<Message>>> map = Maps.of();
		msgs.eachs(m -> map.compute(m.table(), (core, cores) -> {
			if (null == cores) cores = Maps.of();
			cores.compute(m.op(), (op, ops) -> {
				if (null == ops) ops = Colls.list();
				ops.add(m);
				return ops;
			});
			return cores;
		}));
		for (String core : map.keySet())
			for (int op : map.get(core).keySet()) {
				List<Message> ms = map.get(core).get(op);
				try {
					switch (op) {
					case Op.DELETE:
						solr.client().deleteById(core, Sdream.of(map.get(core).get(op)).map(Message::key).list(), DEFAULT_AUTO_COMMIT_MS);
						break;
					default:
						solr.client().add(core, Sdream.of(map.get(core).get(op)).map(m -> Solrs.input(m, keyFieldName)).list(),
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
			solr.client().commit(false, false, true);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
