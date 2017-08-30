package net.butfly.albatis.solr;

import static net.butfly.albacore.io.utils.Streams.list;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import com.hzcominfo.albatis.nosql.Connection;

import net.butfly.albacore.io.Message;
import net.butfly.albacore.io.faliover.FailoverOutput;
import net.butfly.albacore.io.utils.URISpec;
import net.butfly.albacore.utils.Exceptions;

public final class SolrOutput extends FailoverOutput {
	static final int DEFAULT_AUTO_COMMIT_MS = 30000;
	private final SolrConnection solr;

	public SolrOutput(String name, URISpec uri) throws IOException {
		this(name, uri, null);
	}

	public SolrOutput(String name, URISpec uri, String failoverPath) throws IOException {
		super(name, failoverPath, Integer.parseInt(uri.getParameter(Connection.PARAM_KEY_BATCH, "200")));
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

	@SuppressWarnings("unchecked")
	@Override
	protected <M extends Message> long write(String core, Stream<M> msgs, Consumer<Collection<M>> failing, Consumer<Long> committing,
			int retry) {
		List<SolrInputDocument> ds = list(msgs, d -> {
			SolrInputDocument sd = new SolrInputDocument();
			d.entrySet().forEach(e -> sd.addField(e.getKey(), e.getValue()));
			return sd;
		});
		if (ds.isEmpty()) return 0;
		long rr = 0;
		try {
			solr.client().add(core, ds, DEFAULT_AUTO_COMMIT_MS);
			rr = ds.size();
		} catch (Exception ex) {
			logger().warn(name() + " write failed [" + Exceptions.unwrap(ex).getMessage() + "], [" + ds.size() + "] into failover.");
			failing.accept(list(ds, sid -> {
				Message m = new Message(core);
				sid.getFieldNames().forEach(f -> m.put(f, sid.getFieldValue(f)));
				return (M) m;
			}));
		} finally {
			committing.accept(rr);
		}
		return rr;
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
