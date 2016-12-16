package net.butfly.albatis.solr;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import net.butfly.albacore.io.OpenableBasic;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;

class SolrSender extends OpenableBasic implements Runnable {
	private static final Logger logger = Logger.getLogger(SolrSender.class);
	private final int seq;
	private final Thread sender;
	private final String solrName;
	private LinkedBlockingQueue<Runnable> tasks;

	public SolrSender(String solrName, LinkedBlockingQueue<Runnable> tasks, int i) {
		super();
		this.seq = i;
		this.solrName = solrName;
		this.tasks = tasks;
		sender = new Thread(this, solrName + "-Sender-" + i);
		sender.setUncaughtExceptionHandler((tt, e) -> logger.error(solrName + "-Sender failure in async", e));
		sender.start();
	}

	@Override
	public void close() {
		while (sender.isAlive())
			Concurrents.waitSleep(100);
	}

	@Override
	public void run() {
		while (status.get() == Status.OPENED) {
			Runnable r = null;
			try {
				r = tasks.poll(100, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				logger.warn(solrName + "-Sender interrupted.");
			}
			if (null != r) try {
				r.run();
			} catch (Exception e) {
				logger.error("SolrOutput [" + solrName + "] failure", e);
			}
		}
		logger.debug("SolrOutput [" + solrName + "] processing [" + seq + "] finishing");
		Runnable remained;
		while ((remained = tasks.poll()) != null)
			try {
				remained.run();
			} catch (Exception e) {
				logger.error("SolrOutput [" + solrName + "] failure", e);
			}
		logger.info("SolrOutput [" + solrName + "] processing [" + seq + "] finished");
	}
}