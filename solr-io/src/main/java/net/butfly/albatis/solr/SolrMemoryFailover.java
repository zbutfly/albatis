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
	private static final int MAX_FAILOVER = 50000;
	private Map<String, LinkedBlockingQueue<SolrInputDocument>> failover = new ConcurrentHashMap<>();

	public SolrMemoryFailover(SolrOutput solr) throws IOException {
		super(solr);
		logger.info(MessageFormat.format("SolrOutput [{0}] failover in memory mode.", solr.name()));
		failover = new ConcurrentHashMap<>();
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
			logger.warn(MessageFormat.format(
					"SolrOutput [{0}] add failure on core [{1}] with [{2}] docs, push to failover, now [{3}] failover on core [{4}]", solr
							.name(), core, docs.size(), fails.size(), core), err);
		} catch (IllegalStateException ex) {
			logger.error(MessageFormat.format("SolrOutput [{0}] failover full, [{1}] docs lost for core [{2}] ", solr.name(), docs.size(),
					core));
		}
	}

	@Override
	public void fail(String core, SolrInputDocument doc, Exception err) {
		LinkedBlockingQueue<SolrInputDocument> fails = failover.computeIfAbsent(core, k -> new LinkedBlockingQueue<>(MAX_FAILOVER));
		if (fails.offer(doc)) logger.warn("SolrOutput [" + solr.name() + "] add failure on core [" + core + "] "//
				+ "with [1] docs, "//
				+ "push to failover, now [" + fails.size() + "] failover on core [" + core + "]", err);
		else logger.error("SolrOutput [" + solr.name() + "] failover full, [1] docs lost for core [" + core + "] ");
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
				try {
					fails.addAll(retries);
					logger.warn(MessageFormat.format(
							"SolrOutput [{0}] retry failure on core [{1}] with [{2}] docs, push back to failover, now [{3}] failover on core [{4}]",
							solr.name(), core, retries.size(), fails.size(), core), err);
				} catch (IllegalStateException ex) {
					logger.error(MessageFormat.format("SolrOutput [{0}] failover full, [{1}] docs lost for core [{2}] ", solr.name(),
							retries.size(), core));
				}
			}
		}
		if (remained > 0) logger.trace(MessageFormat.format("SolrOutput [{0}] retried, failover remained: [{1}].", solr.name(), remained));
	}
}