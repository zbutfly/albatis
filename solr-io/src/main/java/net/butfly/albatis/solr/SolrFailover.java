package net.butfly.albatis.solr;

import java.util.List;

import org.apache.solr.common.SolrInputDocument;

import net.butfly.albacore.io.OpenableThread;
import net.butfly.albacore.io.stats.Statistical;
import net.butfly.albacore.lambda.ConverterPair;
import net.butfly.albacore.utils.logger.Logger;

abstract class SolrFailover extends OpenableThread implements Statistical<SolrFailover, SolrInputDocument> {
	private static final long serialVersionUID = -7515454826294115208L;
	protected static final Logger logger = Logger.getLogger(SolrFailover.class);
	protected final ConverterPair<String, List<SolrInputDocument>, Exception> adding;

	SolrFailover(String solrName, ConverterPair<String, List<SolrInputDocument>, Exception> adding) {
		super(solrName + "-Failover");
		this.adding = adding;
		trace(solrName + "-Failover", SolrOutput.DEFAULT_PACKAGE_SIZE, m -> 0L, () -> "failover: " + size());
	}

	@Override
	public void run() {
		while (opened())
			failover();
	}

	public abstract boolean isEmpty();

	public abstract long size();

	public abstract int fail(String core, List<SolrInputDocument> docs, Exception err);

	public abstract boolean fail(String core, SolrInputDocument doc, Exception err);

	protected abstract void failover();
}
