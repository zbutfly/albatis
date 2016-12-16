package net.butfly.albatis.solr;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.solr.common.SolrInputDocument;

import net.butfly.albacore.lambda.ConverterPair;

class SolrFailoverMemory extends SolrFailover {
	private static final long serialVersionUID = -7766759011944551301L;
	private static final int MAX_FAILOVER = 50000;
	private Map<String, LinkedBlockingQueue<SolrInputDocument>> failover = new ConcurrentHashMap<>();

	public SolrFailoverMemory(String solrName, ConverterPair<String, List<SolrInputDocument>, Exception> adding) throws IOException {
		super(solrName, adding);
		failover = new ConcurrentHashMap<>();
		logger.info(MessageFormat.format("SolrOutput [{0}] failover [memory mode] init.", solrName));
	}

	@Override
	public long size() {
		long c = 0;
		for (String core : failover.keySet())
			c += failover.get(core).size();
		return c;
	}

	@Override
	public boolean isEmpty() {
		return failover.isEmpty();
	}

	@Override
	public int fail(String core, List<SolrInputDocument> docs, Exception err) {
		LinkedBlockingQueue<SolrInputDocument> fails = failover.computeIfAbsent(core, k -> new LinkedBlockingQueue<>(MAX_FAILOVER));
		try {
			fails.addAll(docs);
			if (null != err) logger.warn(MessageFormat.format(
					"Failure added on [{0}] with [{1}] docs, now [{2}] failover on [{0}], caused by [{3}]", //
					core, docs.size(), size(), err.getMessage()));
			return docs.size();
		} catch (IllegalStateException ee) {
			if (null != err) logger.error(MessageFormat.format("Failover failed, [{0}] docs lost on [{1}], original caused by [{2}]", //
					docs.size(), core, err.getMessage()));
			else logger.error(MessageFormat.format("Failover failed, [{0}] docs lost on [{1}]", docs.size(), core));
			return 0;
		}
	}

	@Override
	public boolean fail(String core, SolrInputDocument doc, Exception err) {
		LinkedBlockingQueue<SolrInputDocument> fails = failover.computeIfAbsent(core, k -> new LinkedBlockingQueue<>(MAX_FAILOVER));
		boolean r = fails.offer(doc);
		if (r) {
			if (null != err) logger.warn(MessageFormat.format(
					"Failure added on [{0}] with [{1}] docs, now [{2}] failover on [{0}], caused by [{3}]", //
					core, 1, size(), err.getMessage()));
		} else {
			if (null != err) logger.error(MessageFormat.format("Failover failed, [{0}] docs lost on [{1}], original caused by [{2}]", //
					1, core, err.getMessage()));
			else logger.error(MessageFormat.format("Failover failed, [{0}] docs lost on [{1}]", 1, core));
		}
		return r;
	}

	@Override
	protected void failover() {
		List<SolrInputDocument> retries = new ArrayList<>(SolrOutput.DEFAULT_PACKAGE_SIZE);
		for (String core : failover.keySet()) {
			LinkedBlockingQueue<SolrInputDocument> fails = failover.get(core);
			fails.drainTo(retries, SolrOutput.DEFAULT_PACKAGE_SIZE);
			stats(retries);
			if (!retries.isEmpty()) {
				Exception e = adding.apply(core, retries);
				if (null != e) fail(core, retries, e);
				retries.clear();
			}
		}
	}

}