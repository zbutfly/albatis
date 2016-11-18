package net.butfly.albatis.solr;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import com.google.common.collect.Lists;

import net.butfly.albacore.io.OutputQueue;
import net.butfly.albacore.io.OutputQueueImpl;
import net.butfly.albacore.utils.logger.Logger;

public class SolrOutput extends OutputQueueImpl<SolrInputDocument> implements OutputQueue<SolrInputDocument> {
	private static final long serialVersionUID = -2897525987426418020L;
	private static final Logger logger = Logger.getLogger(SolrOutput.class);
	private static final int AUTO_COMMIT_MS = 30000;
	private final SolrClient solr;
	private final String core;

	public SolrOutput(final String name, final String url) throws IOException {
		this(name, url, null);
	}

	public SolrOutput(final String name, final String url, String core) throws IOException {
		super(name);
		logger.info("SolrOutput [" + name + "] from [" + url + "], core [" + core + "]");
		solr = Solrs.open(url);
		this.core = core;
	}

	@Override
	protected boolean enqueueRaw(SolrInputDocument d) {
		try {
			UpdateResponse resp = null == core ? solr.add(d, AUTO_COMMIT_MS) : solr.add(core, d, AUTO_COMMIT_MS);
			return (resp.getStatus() == 0);
		} catch (SolrServerException | IOException e) {
			logger.error("SolrOutput sent not successed", e);
			return false;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public long enqueue(Iterator<SolrInputDocument> iter) {
		if (!iter.hasNext()) return 0;
		Collection<SolrInputDocument> c;
		if (iter instanceof Collection) c = (Collection<SolrInputDocument>) iter;
		else c = Lists.newArrayList(iter);
		try {
			UpdateResponse resp = null == core ? solr.add(c, AUTO_COMMIT_MS) : solr.add(core, c, AUTO_COMMIT_MS);
			if (resp.getStatus() == 0) {
				logger.trace("SolrOutput: " + c.size() + " docs in " + resp.getQTime() + " ms.");
				return c.size();
			}
		} catch (SolrServerException | IOException e) {
			logger.error("SolrOutput sent not successed", e);
		}
		return 0;
	}

	@Override
	public void close() {
		logger.debug("SolrOutput [" + name() + "] closing...");
		try {
			if (null == core) solr.commit();
			else solr.commit(core);
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
