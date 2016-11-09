package net.butfly.albatis.solr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.common.SolrInputDocument;

import net.butfly.albacore.io.OutputQueue;
import net.butfly.albacore.io.OutputQueueImpl;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Collections;

public class SolrOutput<T> extends OutputQueueImpl<T, SolrInputDocument> implements OutputQueue<T> {
	private static final long serialVersionUID = -2897525987426418020L;
	private final SolrClient solr;

	public SolrOutput(final String url, final Converter<T, SolrInputDocument> conv) throws IOException {
		this(url, conv, 1);
	}

	public SolrOutput(final String url, final Converter<T, SolrInputDocument> conv, final int parallelism) throws IOException {
		this(url, conv, parallelism, 4096);
	}

	public SolrOutput(final String url, final Converter<T, SolrInputDocument> conv, final int parallelism, final int outputBatchBytes)
			throws IOException {
		super("solr-output-queue", conv);
		if (parallelism <= 1) solr = new CloudSolrClient(url);
		else solr = new ConcurrentUpdateSolrClient(url, outputBatchBytes, parallelism);
	}

	@Override
	protected boolean enqueueRaw(T s) {
		int status;
		try {
			SolrInputDocument d = conv.apply(s);
			if (null == d) return false;
			status = solr.add(d).getStatus();
			return status >= 200 && status < 300;
		} catch (SolrServerException | IOException e) {
			logger.error("Solr sent not successed", e);
			return false;
		}
	}

	@Override
	public long enqueue(Iterator<T> iter) {
		// XXX
		try {
			int status = solr.add(Collections.transform(iter, conv)).getStatus();
			if (status >= 200 && status < 300) return Long.MAX_VALUE;
		} catch (SolrServerException | IOException e) {
			logger.error("Solr sent not successed", e);
		}
		return 0;
	}
}
