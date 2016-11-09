package net.butfly.albatis.solr;

import java.io.IOException;
import java.util.List;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import net.butfly.albacore.io.InputQueueImpl;
import net.butfly.albacore.utils.logger.Logger;

public class SolrInput extends InputQueueImpl<SolrDocument, SolrDocument> {
	private static final long serialVersionUID = -1756036095144276450L;
	private static final Logger logger = Logger.getLogger(SolrInput.class);
	private final SolrClient solr;

	public SolrInput(final String solrZookeeper) throws IOException, SolrServerException {
		this(solrZookeeper, (String) null);
	}

	public SolrInput(final String solrZookeeper, String collection) throws IOException, SolrServerException {
		this(solrZookeeper, collection, new SolrQuery());
	}

	public SolrInput(final String solrZookeeper, final SolrQuery filter) throws IOException, SolrServerException {
		this(solrZookeeper, null, filter);
	}

	public SolrInput(final String solrZookeeper, String collection, final SolrQuery filter) throws IOException, SolrServerException {
		super("solr-input-queue");
		solr = new CloudSolrClient(solrZookeeper);
		QueryResponse r = solr.query(collection, filter);
		SolrDocumentList sds = r.getResults();
	}

	@Override
	public void close() {
		try {
			solr.close();
		} catch (IOException e) {
			logger.error("Solr close failure", e);
		}
	}

	@Override
	public long size() {
		return 0;
	}

	@Override
	public boolean empty() {
		return true;
	}

	@Override
	protected SolrDocument dequeueRaw() {
		return null;
	}

	@Override
	public SolrDocument dequeue() {
		return super.dequeue();
	}

	@Override
	public List<SolrDocument> dequeue(long batchSize) {
		return super.dequeue(batchSize);
	}
}
