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
import java.util.concurrent.ConcurrentHashMap;
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
	private static final int DEFAULT_AUTO_COMMIT_MS = 30000;
	private static final int DEFAULT_PACKAGE_SIZE = 500;
	private static final int DEFAULT_PARALLELISM = 10;
	private static final int MAX_FAILOVER = 50000;
	private final SolrClient solr;
	private final Set<String> cores;
	private final ListeningExecutorService ex;
	private final AtomicBoolean failoverRetry;

	// for reconnect
	private final Map<String, LinkedBlockingQueue<SolrInputDocument>> failover = new ConcurrentHashMap<>();

	public SolrOutput(String name, String baseUrl) throws IOException {
		super(name, m -> m.getCore());
		logger.info("SolrOutput [" + name + "] from [" + baseUrl + "]");
		ex = Concurrents.executor(DEFAULT_PARALLELISM, new ThreadFactoryBuilder().setNameFormat(name + "-Sender-%d").setPriority(
				Thread.MAX_PRIORITY).setUncaughtExceptionHandler((t, e) -> logger.error("SolrOutput failure in async", e)).build());
		Tuple3<String, String, String[]> t;
		try {
			t = Solrs.parseSolrURL(baseUrl);
		} catch (SolrServerException | URISyntaxException e) {
			throw new RuntimeException(e);
		}
		cores = new HashSet<>(Arrays.asList(t._3()));
		solr = Solrs.open(baseUrl);
		failoverRetry = new AtomicBoolean(true);
		new Thread() {
			@Override
			public void run() {
				List<SolrInputDocument> retries = new ArrayList<>(DEFAULT_PACKAGE_SIZE);
				while (failoverRetry.get())
					for (String core : failover.keySet()) {
						LinkedBlockingQueue<SolrInputDocument> fails = failover.get(core);
						while (!fails.isEmpty() && retries.size() < DEFAULT_PACKAGE_SIZE)
							retries.add(fails.poll());
						if (!retries.isEmpty()) try {
							solr.add(core, retries);
							retries.clear();
						} catch (Exception err) {
							try {
								fails.addAll(retries);
								logger.warn("SolrOutput [" + name() + "] retry failure on core [" + core + "] "//
										+ "with [" + retries.size() + "] docs, "//
										+ "push back to failover, now [" + fails.size() + "] failover on core [" + core + "]", err);
							} catch (IllegalStateException ex) {
								logger.error("SolrOutput [" + name() + "] failover full, [" + retries.size() + "] docs lost for core ["
										+ core + "] ");
							}
						}
					}
			}
		}.start();
	}

	@Override
	public boolean enqueue0(String core, SolrMessage<SolrInputDocument> doc) {
		cores.add(core);
		ex.submit(() -> {
			try {
				solr.add(core, doc.getDoc(), DEFAULT_AUTO_COMMIT_MS);
			} catch (Exception err) {
				LinkedBlockingQueue<SolrInputDocument> fails = failover.computeIfAbsent(core, k -> new LinkedBlockingQueue<>(MAX_FAILOVER));
				if (fails.offer(doc.getDoc())) logger.warn("SolrOutput [" + name() + "] add failure on core [" + core + "] "//
						+ "with [1] docs, "//
						+ "push to failover, now [" + fails.size() + "] failover on core [" + core + "]", err);
				else logger.error("SolrOutput [" + name() + "] failover full, [1] docs lost for core [" + core + "] ");
			}
		});
		return true;
	}

	@Override
	public long enqueue(Converter<SolrMessage<SolrInputDocument>, String> keying, List<SolrMessage<SolrInputDocument>> docs) {
		Map<String, List<SolrInputDocument>> map = new HashMap<>();
		for (SolrMessage<SolrInputDocument> d : docs)
			map.computeIfAbsent(d.getCore(), core -> new ArrayList<>()).add(d.getDoc());
		cores.addAll(map.keySet());
		for (Entry<String, List<SolrInputDocument>> e : map.entrySet())
			ex.submit(() -> {
				for (List<SolrInputDocument> pkg : Collections.chopped(e.getValue(), DEFAULT_PACKAGE_SIZE)) {
					try {
						solr.add(e.getKey(), pkg);
					} catch (Exception err) {
						LinkedBlockingQueue<SolrInputDocument> fails = failover.computeIfAbsent(e.getKey(), k -> new LinkedBlockingQueue<>(
								MAX_FAILOVER));
						try {
							fails.addAll(pkg);
							logger.warn("SolrOutput [" + name() + "] add failure on core [" + e.getKey() + "] "//
									+ "with [" + pkg.size() + "] docs, "//
									+ "push to failover, now [" + fails.size() + "] failover on core [" + e.getKey() + "]", err);
						} catch (IllegalStateException ex) {
							logger.error("SolrOutput [" + name() + "] failover full, [" + pkg.size() + "] docs lost for core [" + e.getKey()
									+ "] ");
						}
					}
				}
				try {
					solr.commit(e.getKey(), false, false, true);
				} catch (Exception err) {
					logger.warn("SolrOutput [" + name() + "] commit failure on core [" + e.getKey() + "]", err);
				}
			});
		System.gc();
		return docs.size();
	}

	@Override
	public void close() {
		ex.shutdown();
		Concurrents.waitShutdown(ex, logger);
		logger.debug("SolrOutput [" + name() + "] closing...");
		failoverRetry.set(false);
		if (!failover.isEmpty()) logger.error("SolrOutput [" + name() + "] contain faiover [" + failover.size() + "].");
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

	public int fails() {
		return failover.size();
	}
}
