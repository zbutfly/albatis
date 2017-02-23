package net.butfly.albatis.solr;

import static net.butfly.albacore.utils.Exceptions.unwrap;

import java.io.IOException;
import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import net.butfly.albacore.io.IO;
import net.butfly.albacore.io.Message;
import net.butfly.albacore.io.Streams;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.faliover.Failover.FailoverException;
import net.butfly.albacore.io.faliover.FailoverOutput;

public final class SolrOutput extends FailoverOutput<String, SolrMessage<SolrInputDocument>> {
	static final int DEFAULT_AUTO_COMMIT_MS = 30000;
	private final SolrConnection solr;

	public SolrOutput(String name, URISpec uri) throws IOException {
		this(name, uri, null);
	}

	public SolrOutput(String name, URISpec uri, String failoverPath) throws IOException {
		super(name, b -> Message.<SolrMessage<SolrInputDocument>> fromBytes(b), failoverPath, Integer.parseInt(uri.getParameter("batch",
				"200")));
		solr = new SolrConnection(uri);
		open();
	}

	@Override
	protected void closeInternal() {
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
	protected int write(String core, Collection<SolrMessage<SolrInputDocument>> docs) throws FailoverException {
		try {
			solr.client().add(core, IO.list(docs, SolrMessage<SolrInputDocument>::forWrite), DEFAULT_AUTO_COMMIT_MS);
		} catch (Exception e) {
			throw new FailoverException(IO.collect(Streams.of(docs), Collectors.toConcurrentMap(r -> r, r -> unwrap(e).getMessage())));
		}
		return docs.size();
	}

	@Override
	protected void commit(String key) {
		try {
			solr.client().commit(key, false, false, true);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
