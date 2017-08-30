package net.butfly.albatis.solr;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import net.butfly.albacore.io.EnqueueException;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Streams;
import net.butfly.albatis.io.KeyOutput;
import net.butfly.albatis.io.Message;

public final class SolrOutput extends KeyOutput<String, Message> {
	public static final int SUGGEST_BATCH_SIZE = 200;
	private static final int DEFAULT_AUTO_COMMIT_MS = 30000;
	private final SolrConnection solr;

	public SolrOutput(String name, URISpec uri) throws IOException {
		super(name);
		// Integer.parseInt(uri.getParameter(Connection.PARAM_KEY_BATCH, "200"))
		solr = new SolrConnection(uri);
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
	public long enqueue(String core, Stream<Message> msgs) {
		List<SolrInputDocument> docs = Streams.list(msgs.map(m -> Solrs.input(m)));
		if (docs.isEmpty()) return 0;
		try {
			solr.client().add(core, docs, DEFAULT_AUTO_COMMIT_MS);
			return docs.size();
		} catch (Exception ex) {
			EnqueueException e = new EnqueueException();
			e.fails(docs);
			throw e;
		}
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
	protected String partitionize(Message v) {
		return v.key();
	}
}
