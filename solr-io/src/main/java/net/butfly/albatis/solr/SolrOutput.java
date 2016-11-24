package net.butfly.albatis.solr;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import net.butfly.albacore.io.MapOutputImpl;
import net.butfly.albacore.io.queue.Q;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.logger.Logger;

public class SolrOutput extends MapOutputImpl<String, SolrMessage<SolrInputDocument>> {
	private static final long serialVersionUID = -2897525987426418020L;
	private static final Logger logger = Logger.getLogger(SolrOutput.class);
	private static final int AUTO_COMMIT_MS = 30000;
	private final SolrClient solr;

	public SolrOutput(final String name, final String baseUrl) throws IOException {
		super(name, m -> m.getCore());
		logger.info("SolrOutput [" + name + "] from [" + baseUrl + "]");
		solr = Solrs.open(baseUrl);
	}

	@Override
	public long enqueue(String core, Iterator<SolrMessage<SolrInputDocument>> docs) {
		List<SolrInputDocument> ds = Collections.transform(docs, d -> d.getDoc());
		try {
			return solr.add(core, ds, AUTO_COMMIT_MS).getStatus() == 0 ? ds.size() : 0;
		} catch (SolrServerException | IOException e) {
			logger.error("SolrOutput sent not successed", e);
			return 0;
		}
	}

	@Override
	public void close() {
		logger.debug("SolrOutput [" + name() + "] closing...");
		try {
			solr.commit();
		} catch (IOException | SolrServerException e) {
			logger.error("SolrOutput close failure", e);
		}
		try {
			solr.close();
		} catch (IOException e) {
			logger.error("SolrOutput close failure", e);
		}
		logger.info("SolrOutput [" + name() + "] closed.");
	}

	@Override
	public Set<String> keys() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Q<SolrMessage<SolrInputDocument>, Void> q(String key) {
		throw new UnsupportedOperationException();
	}
}
