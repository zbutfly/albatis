package net.butfly.albatis.solr;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.solr.common.SolrInputDocument;

class SolrMemoryFailover extends SolrFailover {
	private static final long serialVersionUID = -7766759011944551301L;
	private static final int MAX_FAILOVER = 50000;
	private Map<String, LinkedBlockingQueue<SolrInputDocument>> failover = new ConcurrentHashMap<>();

	public SolrMemoryFailover(SolrOutput solr) throws IOException {
		super(solr);
		failover = new ConcurrentHashMap<>();
		logger.info(MessageFormat.format("SolrOutput [{0}] failover [memory mode] init.", solr.name()));
	}

	@Override
	public long size() {
		return failover.size();
	}

	@Override
	public boolean isEmpty() {
		return failover.isEmpty();
	}

	@Override
	public void fail(String core, List<SolrInputDocument> docs, Exception err) {
		LinkedBlockingQueue<SolrInputDocument> fails = failover.computeIfAbsent(core, k -> new LinkedBlockingQueue<>(MAX_FAILOVER));
		try {
			fails.addAll(docs);
			if (null != err) logger.warn(MessageFormat.format(
					"SolrOutput [{0}] add failure on [{1}] with [{2}] docs, push to failover, now [{3}] failover on [{1}], failure for [{4}]",
					solr.name(), core, docs.size(), fails.size(), err.getMessage()));
		} catch (IllegalStateException ex) {
			logger.error(MessageFormat.format("SolrOutput [{0}] failover full, [{1}] docs lost on [{2}]", solr.name(), docs.size(), core),
					null == err ? ex : err);
		}
	}

	@Override
	public void fail(String core, SolrInputDocument doc, Exception err) {
		LinkedBlockingQueue<SolrInputDocument> fails = failover.computeIfAbsent(core, k -> new LinkedBlockingQueue<>(MAX_FAILOVER));
		if (fails.offer(doc) && null != err) logger.warn(MessageFormat.format(
				"SolrOutput [{0}] add failure on [{1}] with [{2}] docs, push to failover, now [{3}] failover on [{1}], failure for [{4}]",
				solr.name(), core, 1, fails.size(), err.getMessage()));
		else {
			if (null != err) logger.error(MessageFormat.format("SolrOutput [{0}] failover full, [1] docs lost on [{1}]", solr.name(), core),
					err);
			else logger.error(MessageFormat.format("SolrOutput [{0}] failover full, [1] docs lost on [{1}]", solr.name(), core));
		}
	}

	@Override
	protected void failover() {
		int remained = 0;
		List<SolrInputDocument> retries = new ArrayList<>(SolrOutput.DEFAULT_PACKAGE_SIZE);
		for (String core : failover.keySet()) {
			LinkedBlockingQueue<SolrInputDocument> fails = failover.get(core);
			fails.drainTo(retries, SolrOutput.DEFAULT_PACKAGE_SIZE);
			remained += fails.size();
			if (!retries.isEmpty()) try {
				solr.solr.add(core, retries);
				retries.clear();
			} catch (Exception err) {
				fail(core, retries, err);
			}
		}
		if (remained > 0) logger.debug(MessageFormat.format("SolrOutput [{0}] retried, failover remained: [{1}].", solr.name(), remained));
	}
}