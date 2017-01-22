package net.butfly.albatis.solr;

import net.butfly.albacore.io.faliover.FailoverOutput;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;

public class SolrOutput extends FailoverOutput<SolrMessage<SolrInputDocument>, SolrInputDocument> {
	private static final long serialVersionUID = -2897525987426418020L;
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
	public void close() {
		super.close(this::closeSolr);
	}

	private void closeSolr() {
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
	protected Exception write(String key, List<SolrInputDocument> values) {
		try {
			solr.client().add(key, values, DEFAULT_AUTO_COMMIT_MS);
			return null;
		} catch (Exception e) {
			return e;
		}
	}

	@Override
	protected void commit(String key) {
		try {
			solr.client().commit(key, false, false, true);
		} catch (SolrServerException | IOException e) {
			throw new RuntimeException(e);
		}
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
