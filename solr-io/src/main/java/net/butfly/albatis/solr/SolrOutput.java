package net.butfly.albatis.solr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import net.butfly.albacore.io.OutputQueue;
import net.butfly.albacore.io.OutputQueueImpl;
import net.butfly.albacore.utils.logger.Logger;

public class SolrOutput extends OutputQueueImpl<SolrMessage<SolrInputDocument>> implements OutputQueue<SolrMessage<SolrInputDocument>> {
	private static final long serialVersionUID = -2897525987426418020L;
	private static final Logger logger = Logger.getLogger(SolrOutput.class);
	private static final int AUTO_COMMIT_MS = 30000;
	private final SolrClient solr;

	public SolrOutput(final String name, final String url) throws IOException {
		super(name);
		logger.info("SolrOutput [" + name + "] from [" + url + "]");
		solr = Solrs.open(url);
	}

	@Override
	protected boolean enqueueRaw(SolrMessage<SolrInputDocument> d) {
		try {
			UpdateResponse resp = null == d.getCore() ? solr.add(d.getDoc(), AUTO_COMMIT_MS)
					: solr.add(d.getCore(), d.getDoc(), AUTO_COMMIT_MS);
			return (resp.getStatus() == 0);
		} catch (SolrServerException | IOException e) {
			logger.error("SolrOutput sent not successed", e);
			return false;
		}
	}

	@Override
	public long enqueue(Iterator<SolrMessage<SolrInputDocument>> iter) {
		if (!iter.hasNext()) return 0;
		Map<String, List<SolrInputDocument>> cores = new HashMap<>();
		long t = 0;
		while (iter.hasNext()) {
			SolrMessage<SolrInputDocument> m = iter.next();
			cores.computeIfAbsent(m.getCore(), c -> new ArrayList<>()).add(m.getDoc());
		}
		for (Entry<String, List<SolrInputDocument>> e : cores.entrySet()) {
			UpdateResponse resp;
			try {
				resp = solr.add(e.getKey(), e.getValue(), AUTO_COMMIT_MS);
				if (resp.getStatus() != 0) logger.error("SolrOutput failure " + resp.getStatus());
				t += e.getValue().size();
			} catch (SolrServerException | IOException ex) {
				logger.error("SolrOutput failure", ex);
			}
		}
		return t;
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
}
