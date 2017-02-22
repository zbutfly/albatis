package net.butfly.albatis.solr;

import static net.butfly.albacore.utils.Exceptions.unwrap;

import java.io.IOException;
import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import net.butfly.albacore.io.Message;
import net.butfly.albacore.io.Streams;
import net.butfly.albacore.io.faliover.Failover.FailoverException;
import net.butfly.albacore.io.faliover.FailoverOutput;
import net.butfly.albacore.utils.Configs;

public final class SolrOutput extends FailoverOutput<String, SolrMessage<SolrInputDocument>> {
	private final static int PACKAGE_SIZE = Integer.parseInt(Configs.MAIN_CONF.getOrDefault("albatis.io.solr.batch.size", "500"));
	static final int DEFAULT_AUTO_COMMIT_MS = 30000;
	private final SolrConnection solr;

	public SolrOutput(String name, String baseUrl) throws IOException {
		this(name, baseUrl, null);
		open();
	}

	public SolrOutput(String name, String baseUrl, String failoverPath) throws IOException {
		super(name, b -> Message.<SolrMessage<SolrInputDocument>> fromBytes(b), failoverPath, PACKAGE_SIZE);
		logger().info("[" + name + "] from [" + baseUrl + "]");
		solr = new SolrConnection(baseUrl);
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
			solr.client().add(core, io.list(docs, SolrMessage<SolrInputDocument>::forWrite), DEFAULT_AUTO_COMMIT_MS);
		} catch (Exception e) {
			throw new FailoverException(io.collect(Streams.of(docs), Collectors.toConcurrentMap(r -> r, r -> unwrap(e).getMessage())));
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
