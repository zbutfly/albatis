package net.butfly.albatis.solr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import net.butfly.albacore.io.OutputQueue;
import net.butfly.albacore.io.OutputQueueImpl;
import net.butfly.albacore.utils.logger.Logger;

public class SolrOutput extends OutputQueueImpl<SolrInputDocument> implements OutputQueue<SolrInputDocument> {
	private static final long serialVersionUID = -2897525987426418020L;
	private static final Logger logger = Logger.getLogger(SolrOutput.class);
	private final SolrClient solr;
	private final String core;

	public SolrOutput(final String name, final String url) throws IOException {
		this(name, url, null);
	}

	public SolrOutput(final String name, final String url, String core) throws IOException {
		super(name);
		logger.info("SolrOutput: [" + url + "], with core: [" + core + "]");
		solr = Solrs.open(url);
		this.core = core;
	}

	@Override
	protected boolean enqueueRaw(SolrInputDocument d) {
		try {
			if (null == d) return false;
			UpdateResponse resp = null == core ? solr.add(d) : solr.add(core, d);
			return resp.getStatus() >= 200 && resp.getStatus() < 300;
		} catch (SolrServerException | IOException e) {
			logger.error("Solr sent not successed", e);
			return false;
		}
	}

	@Override
	public long enqueue(Iterator<SolrInputDocument> iter) {
		if (!iter.hasNext()) return 0;
		try {
			UpdateResponse resp = null == core ? solr.add(iter) : solr.add(core, iter);
			if (resp.getStatus() >= 200 && resp.getStatus() < 300) return 1;
		} catch (SolrServerException | IOException e) {
			logger.error("Solr sent not successed", e);
		}
		return 0;
	}

	@Override
	public void close() {
		try {
			solr.close();
		} catch (IOException e) {
			logger.error("Solr close failure", e);
		}
	}
}
