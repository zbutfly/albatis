package net.butfly.albatis.solr;

import java.io.IOException;
import java.util.List;

import org.apache.solr.client.solrj.SolrServerException;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.paral.steam.Steam;
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
	public void enqueue(String core, Steam<Message> msgs) {
		msgs.nonNull().partition((del, s) -> {
			List<Message> ms = s.list();
			try {
				if (del.booleanValue()) solr.client().deleteById(core, Steam.of(ms).map(Message::key).list(), DEFAULT_AUTO_COMMIT_MS);
				else solr.client().add(core, Steam.of(ms).map(m -> Solrs.input(m, keyFieldName)).list(), DEFAULT_AUTO_COMMIT_MS);
				succeeded(ms.size());
			} catch (Exception ex) {
				failed(Steam.of(ms));
			}
		}, m -> Op.DELETE == m.op(), Integer.MAX_VALUE);
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
