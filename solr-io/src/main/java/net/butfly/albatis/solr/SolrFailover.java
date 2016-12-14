package net.butfly.albatis.solr;

import java.util.List;

import org.apache.solr.common.SolrInputDocument;

import net.butfly.albacore.io.stats.Statistical;
import net.butfly.albacore.utils.logger.Logger;

abstract class SolrFailover extends Thread implements Statistical<SolrFailover, SolrInputDocument>, AutoCloseable {
	private static final long serialVersionUID = -7515454826294115208L;
	protected static final Logger logger = Logger.getLogger(SolrFailover.class);
	protected final SolrOutput solr;

	SolrFailover(SolrOutput solr) {
		super();
		this.solr = solr;
		setName("SolrOutputFailover-" + solr.name());
		trace(solr.name() + "_Failover", SolrOutput.DEFAULT_PACKAGE_SIZE, m -> 0L, () -> "failover: " + size());
	}

	@Override
	public void run() {
		logger.info(getName() + " started.");
		while (!solr.closed.get())
			failover();
	}

	public abstract boolean isEmpty();

	public abstract long size();

	public abstract int fail(String core, List<SolrInputDocument> docs, Exception err);

	public abstract boolean fail(String core, SolrInputDocument doc, Exception err);

	protected abstract void failover();

	@Override
	public void close() {}
}
