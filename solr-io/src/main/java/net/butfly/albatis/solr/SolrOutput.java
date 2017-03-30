package net.butfly.albatis.solr;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import com.hzcominfo.albatis.nosql.Connection;

import net.butfly.albacore.io.IO;
import net.butfly.albacore.io.Message;
import net.butfly.albacore.io.faliover.FailoverOutput;
import net.butfly.albacore.io.utils.URISpec;
import net.butfly.albacore.utils.Exceptions;

public final class SolrOutput extends FailoverOutput<String, SolrMessage<SolrInputDocument>> {
	static final int DEFAULT_AUTO_COMMIT_MS = 30000;
	private final SolrConnection solr;

	public SolrOutput(String name, URISpec uri) throws IOException {
		this(name, uri, null);
	}

	public SolrOutput(String name, URISpec uri, String failoverPath) throws IOException {
		super(name, Message::<SolrMessage<SolrInputDocument>> fromBytes, failoverPath, Integer.parseInt(uri.getParameter(
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
	protected long write(String core, Stream<SolrMessage<SolrInputDocument>> docs, Set<SolrMessage<SolrInputDocument>> fails) {
		List<SolrInputDocument> ds = IO.list(docs, SolrMessage<SolrInputDocument>::forWrite);
		if (ds.isEmpty()) return 0;
		try {
			solr.client().add(core, ds, DEFAULT_AUTO_COMMIT_MS);
			return ds.size();
		} catch (Exception ex) {
			logger().warn(name() + " write failed [" + Exceptions.unwrap(ex).getMessage() + "], [" + ds.size() + "] into failover.");
			fails.addAll(IO.list(ds, sid -> new SolrMessage<>(core, sid)));
			return 0;
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
