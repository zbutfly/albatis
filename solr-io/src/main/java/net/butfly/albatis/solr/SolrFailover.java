package net.butfly.albatis.solr;

import java.text.MessageFormat;
import java.util.List;

import org.apache.solr.common.SolrInputDocument;

import net.butfly.albacore.utils.logger.Logger;

abstract class SolrFailover extends Thread {
	protected static final Logger logger = Logger.getLogger(SolrFailover.class);
	protected final SolrOutput solr;

	SolrFailover(SolrOutput solr) {
		super();
		this.solr = solr;

		setName("SolrOutputFailover-" + solr.name());
	}

	@Override
	public void run() {
		while (!solr.closed.get())
			failover();
		long remained = size();
		if (remained > 0) logger.error(MessageFormat.format("SolrOutput [{0}] failover task finished, failover remained : [{1}].", solr
				.name(), remained));
	}

	public abstract boolean isEmpty();

	public abstract long size();

	public abstract void fail(String key, List<SolrInputDocument> docs, Exception err);

	public abstract void fail(String core, SolrInputDocument doc, Exception err);

	protected abstract void failover();

}
