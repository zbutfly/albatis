package net.butfly.albatis.solr;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import net.butfly.albacore.io.MapOutput;
import net.butfly.albacore.io.queue.Q;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;
import scala.Tuple3;

public class SolrOutput extends MapOutput<String, SolrMessage<SolrInputDocument>> {
	private static final long serialVersionUID = -2897525987426418020L;
	private static final Logger logger = Logger.getLogger(SolrOutput.class);
	static final int DEFAULT_AUTO_COMMIT_MS = 30000;
	static final int DEFAULT_PACKAGE_SIZE = 500;
	private static final int DEFAULT_PARALLELISM = 5;
	final SolrClient solr;
	final AtomicBoolean closed;

	private final Set<String> cores;
	private final LinkedBlockingQueue<Runnable> tasks;
	private final ListeningExecutorService ex;
	private final SolrFailover failover;
	private final List<SolrSender> senders;

	public SolrOutput(String name, String baseUrl) throws IOException {
		this(name, baseUrl, null);
	}

	public SolrOutput(String name, String baseUrl, String failoverPath) throws IOException {
		super(name, m -> m.getCore());
		logger.info("SolrOutput [" + name + "] from [" + baseUrl + "]");
		ex = Concurrents.executor(DEFAULT_PARALLELISM, new ThreadFactoryBuilder().setNameFormat("SolrSender-" + name + "-%d").setPriority(
				Thread.MAX_PRIORITY).setUncaughtExceptionHandler((t, e) -> logger.error("SolrOutput [" + name() + "] failure in async", e))
				.build());
		Tuple3<String, String, String[]> t;
		try {
			t = Solrs.parseSolrURL(baseUrl);
		} catch (SolrServerException | URISyntaxException e) {
			throw new IOException(e);
		}
		cores = new HashSet<>(Arrays.asList(t._3()));
		solr = Solrs.open(baseUrl);
		closed = new AtomicBoolean(false);
		tasks = new LinkedBlockingQueue<>(DEFAULT_PARALLELISM);
		failover = failoverPath == null ? new SolrMemoryFailover(this) : new SolrPersistFailover(this, failoverPath, baseUrl);
		failover.start();
		senders = new ArrayList<>();
		for (int i = 0; i < DEFAULT_PARALLELISM; i++) {
			SolrSender s = new SolrSender(i);
			senders.add(s);
			s.start();
		}
	}

	@Override
	public boolean enqueue0(String core, SolrMessage<SolrInputDocument> doc) {
		cores.add(core);
		try {
			tasks.put(() -> {
				try {
					solr.add(core, doc.getDoc(), DEFAULT_AUTO_COMMIT_MS);
				} catch (Exception err) {
					failover.fail(core, doc.getDoc(), err);
				}
			});
		} catch (InterruptedException e) {
			logger.error("SolrOutput [" + name() + "] task submit interrupted", e);
		}
		return true;
	}

	@Override
	public long enqueue(Converter<SolrMessage<SolrInputDocument>, String> keying, List<SolrMessage<SolrInputDocument>> docs) {
		Map<String, List<SolrInputDocument>> map = new HashMap<>();
		for (SolrMessage<SolrInputDocument> d : docs)
			map.computeIfAbsent(d.getCore(), core -> new ArrayList<>()).add(d.getDoc());
		cores.addAll(map.keySet());
		for (Entry<String, List<SolrInputDocument>> e : map.entrySet())
			try {
				tasks.put(() -> {
					for (List<SolrInputDocument> pkg : Collections.chopped(e.getValue(), DEFAULT_PACKAGE_SIZE)) {
						try {
							solr.add(e.getKey(), pkg);
						} catch (Exception err) {
							failover.fail(e.getKey(), pkg, err);
						}
					}
					try {
						solr.commit(e.getKey(), false, false, true);
					} catch (Exception err) {
						logger.warn("SolrOutput [" + name() + "] commit failure on core [" + e.getKey() + "]", err);
					}
				});
			} catch (InterruptedException ee) {
				logger.error("SolrOutput [" + name() + "] task submit interrupted", ee);
			}
		return docs.size();
	}

	@Override
	public void close() {
		ex.shutdown();
		Concurrents.waitShutdown(ex, logger);
		logger.debug("SolrOutput [" + name() + "] closing...");
		closed.set(false);
		try {
			for (String core : cores)
				solr.commit(core, false, false);
		} catch (IOException | SolrServerException e) {
			logger.error("SolrOutput close failure", e);
		}
		Solrs.close(solr);
		logger.info("SolrOutput [" + name() + "] closed.");
	}

	@Override
	public Set<String> keys() {
		return cores;
	}

	@Override
	public Q<SolrMessage<SolrInputDocument>, Void> q(String key) {
		throw new UnsupportedOperationException();
	}

	public long fails() {
		return failover.size();
	}

	private class SolrSender extends Thread {
		private final int seq;

		public SolrSender(int i) {
			super();
			this.seq = i;
			setName("SolrSender-" + SolrOutput.this.name() + "-" + i);
		}

		@Override
		public void run() {
			while (!closed.get()) {
				Runnable r = null;
				try {
					r = tasks.take();
				} catch (InterruptedException e) {
					logger.error("SolrOutput [" + name() + "] interrupted", e);
				}
				if (null != r) try {
					r.run();
				} catch (Exception e) {
					logger.error("SolrOutput [" + name() + "] failure", e);
				}
			}
			logger.info("SolrOutput [" + name() + "] processing [" + seq + "] finishing");
			Runnable remained;
			while ((remained = tasks.poll()) != null)
				try {
					remained.run();
				} catch (Exception e) {
					logger.error("SolrOutput [" + name() + "] failure", e);
				}
			logger.info("SolrOutput [" + name() + "] processing [" + seq + "] finished");
		}
	}
}
