package net.butfly.albatis.solr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import net.butfly.albacore.io.MapOutput;
import net.butfly.albacore.io.queue.Q;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.logger.Logger;

public class SolrOutput extends MapOutput<String, SolrMessage<SolrInputDocument>> {
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
	public boolean enqueue0(String core, SolrMessage<SolrInputDocument> doc) {
		try {
			return solr.add(core, doc.getDoc(), AUTO_COMMIT_MS).getStatus() == 0;
		} catch (SolrServerException | IOException e) {
			logger.error("SolrOutput sent not successed", e);
			return false;
		}
	}

	@Override
	public long enqueue(Converter<SolrMessage<SolrInputDocument>, String> keying, List<SolrMessage<SolrInputDocument>> docs) {
		Map<String, List<SolrInputDocument>> map = new HashMap<>();
		for (SolrMessage<SolrInputDocument> d : docs)
			map.computeIfAbsent(d.getCore(), core -> new ArrayList<>()).add(d.getDoc());
		int count = 0;
		for (Entry<String, List<SolrInputDocument>> e : map.entrySet())
			try {
				if (solr.add(e.getKey(), e.getValue(), AUTO_COMMIT_MS).getStatus() == 0) count += e.getValue().size();
			} catch (SolrServerException | IOException ex) {
				logger.error("SolrOutput sent not successed", ex);
			}
		return count;
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
