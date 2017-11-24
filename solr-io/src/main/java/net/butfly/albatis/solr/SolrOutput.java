package net.butfly.albatis.solr;

import static net.butfly.albacore.utils.collection.Streams.NOT_NULL;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServerException;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.paral.Exeters;
import net.butfly.albatis.io.KeyOutput;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.Message.Op;
import net.butfly.albatis.io.OutputBase;

public final class SolrOutput extends OutputBase<Message> {
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
	public void enqueue(String core, Stream<Message> msgs) {
		ConcurrentMap<Boolean, List<Message>> ops = msgs.filter(NOT_NULL).collect(Collectors.groupingByConcurrent(m -> Op.DELETE == m
				.op()));
		List<Message> del;
		if (null != (del = ops.get(Boolean.TRUE)) && !del.isEmpty()) Exeters.DEFEX.submit(() -> {
			try {
				solr.client().deleteById(core, list(del, Message::key), DEFAULT_AUTO_COMMIT_MS);
				succeeded(del.size());
			} catch (Exception ex) {
				failed(del.parallelStream());
			}
		});
		List<Message> ins;
		if (null != (ins = ops.get(Boolean.FALSE)) && !ins.isEmpty()) Exeters.DEFEX.submit(() -> {
			try {
				solr.client().add(core, list(ins, t -> Solrs.input(t, keyFieldName)), DEFAULT_AUTO_COMMIT_MS);
				succeeded(ins.size());
			} catch (Exception ex) {
				failed(ins.parallelStream());
			}
		});
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

	@Override
	public String partition(Message v) {
		return v.table();
	}
}
