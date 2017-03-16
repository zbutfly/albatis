package net.butfly.albatis.solr;

import static net.butfly.albacore.utils.Exceptions.unwrap;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import com.hzcominfo.albatis.nosql.Connection;

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
		super(name, b -> Message.<SolrMessage<SolrInputDocument>> fromBytes(b), failoverPath, Integer.parseInt(uri.getParameter(
				Connection.PARAM_KEY_BATCH, "200")));
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
	protected long write(String core, Stream<SolrMessage<SolrInputDocument>> docs) throws FailoverException {
		List<SolrInputDocument> ds = IO.list(docs, SolrMessage<SolrInputDocument>::forWrite);
		if (ds.isEmpty()) return 0;
		try {
			solr.client().add(core, ds, DEFAULT_AUTO_COMMIT_MS);
			return ds.size();
		} catch (Exception e) {
			throw new FailoverException(IO.collect(Streams.of(ds), Collectors.toConcurrentMap(r -> r, r -> unwrap(e).getMessage())));
		}
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
