package net.butfly.albatis.solr;

import static net.butfly.albacore.utils.collection.Streams.NOT_NULL;
import static net.butfly.albacore.utils.collection.Streams.list;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.solr.client.solrj.SolrServerException;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.utils.parallel.Parals;
import net.butfly.albatis.io.KeyOutput;
import net.butfly.albatis.io.Message;
import net.butfly.albatis.io.Message.Op;

public final class SolrOutput extends Namedly implements KeyOutput<String, Message> {
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
		KeyOutput.super.close();
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
		if (null != (del = ops.get(Boolean.TRUE)) && !del.isEmpty()) Parals.listenRun(() -> {
			try {
				solr.client().deleteById(core, list(del, Message::key), DEFAULT_AUTO_COMMIT_MS);
				succeeded(del.size());
			} catch (Exception ex) {
				failed(del.parallelStream());
			}
		});
		List<Message> ins;
		if (null != (ins = ops.get(Boolean.FALSE)) && !ins.isEmpty()) Parals.listenRun(() -> {
			try {
				solr.client().add(core, list(ins, t -> Solrs.input(t, keyFieldName)), DEFAULT_AUTO_COMMIT_MS);
				succeeded(ins.size());
			} catch (Exception ex) {
				failed(ins.parallelStream());
			}
		});
	}

	@Override
	public void commit(String key) {
		try {
			solr.client().commit(key, false, false, true);
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
