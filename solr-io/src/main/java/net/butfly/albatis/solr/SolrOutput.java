package net.butfly.albatis.solr;

import java.io.IOException;
import java.util.List;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import net.butfly.albacore.io.faliover.FailoverOutput;
import scala.Tuple2;

public class SolrOutput extends FailoverOutput<SolrMessage<SolrInputDocument>, SolrInputDocument> {
	static final int DEFAULT_AUTO_COMMIT_MS = 30000;
	private final SolrConnection solr;

	public SolrOutput(String name, String baseUrl) throws IOException {
		this(name, baseUrl, null);
	}

	public SolrOutput(String name, String baseUrl, String failoverPath) throws IOException {
		super(name, failoverPath, 500, 5);
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
	protected int write(String key, List<SolrInputDocument> values) {
		try {
			solr.client().add(key, values, DEFAULT_AUTO_COMMIT_MS);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return values.size();
	}

	@Override
	protected void commit(String key) throws Exception {
		solr.client().commit(key, false, false, true);
	}

	@Override
	protected Tuple2<String, SolrInputDocument> parse(SolrMessage<SolrInputDocument> e) {
		return new Tuple2<>(e.getCore() == null ? solr.getDefaultCore() : e.getCore(), e.getDoc());
	}

	@Override
	protected SolrMessage<SolrInputDocument> unparse(String key, SolrInputDocument value) {
		return new SolrMessage<SolrInputDocument>(key, value);
	}
}
