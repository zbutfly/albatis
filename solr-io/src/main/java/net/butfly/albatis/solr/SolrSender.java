package net.butfly.albatis.solr;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import net.butfly.albacore.io.OpenableThread;
import net.butfly.albacore.utils.logger.Logger;

class SolrSender extends OpenableThread {
	private static final Logger logger = Logger.getLogger(SolrSender.class);
	private LinkedBlockingQueue<Runnable> tasks;

	public SolrSender(String solrName, LinkedBlockingQueue<Runnable> tasks, int i) {
		super(solrName + "-Sender-" + (i + 1));
		this.tasks = tasks;
		start();
	}

	@Override
	public void run() {
		while (opened()) {
			Runnable r = null;
			try {
				r = tasks.poll(100, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {}
			if (null != r) try {
				r.run();
			} catch (Exception e) {
				logger.error("Sending failure", e);
			}
		}
		logger.debug("Processing remained");
		Runnable remained;
		while ((remained = tasks.poll()) != null)
			try {
				remained.run();
			} catch (Exception e) {
				logger.error("Sending failure", e);
			}
	}
}