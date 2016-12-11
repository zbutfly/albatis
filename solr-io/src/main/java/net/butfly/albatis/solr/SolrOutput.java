package net.butfly.albatis.solr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import net.butfly.albacore.io.MapOutput;
import net.butfly.albacore.io.queue.Q;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;

public class SolrOutput extends MapOutput<String, SolrMessage<SolrInputDocument>> {
	private static final long serialVersionUID = -2897525987426418020L;
	private static final Logger logger = Logger.getLogger(SolrOutput.class);
	private static final int DEFAULT_AUTO_COMMIT_MS = 30000;
	private static final int DEFAULT_PACKAGE_SIZE = 500;
	private static final int DEFAULT_PARALLELISM = 10;
	private final SolrClient solr;
	private final Set<String> cores;
	private final ExecutorService ex;

	public SolrOutput(String name, String baseUrl) throws IOException {
		super(name, m -> m.getCore());
		logger.info("SolrOutput [" + name + "] from [" + baseUrl + "]");
		ex = Executors.newFixedThreadPool(DEFAULT_PARALLELISM, new ThreadFactoryBuilder().setNameFormat("SolrOutputSender-%d").setPriority(
				Thread.MAX_PRIORITY).setUncaughtExceptionHandler((t, e) -> logger.error("SolrOutput failure in async", e)).build());
		cores = new HashSet<>();
		solr = Solrs.open(baseUrl);
	}

	@Override
	public boolean enqueue0(String core, SolrMessage<SolrInputDocument> doc) {
		cores.add(core);
		ex.submit(() -> solr.add(core, doc.getDoc(), DEFAULT_AUTO_COMMIT_MS));
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
				for (List<SolrInputDocument> pkg : Collections.chopped(e.getValue(), DEFAULT_PACKAGE_SIZE))
					try {
						solr.add(e.getKey(), pkg);
					} catch (Exception err) {
						logger.error("SolrOutput [" + name() + "] add failure on core [" + e.getKey() + "] with [" + pkg.size() + "] docs",
								err);
					}
				try {
					solr.commit(e.getKey());
				} catch (Exception err) {
					logger.error("SolrOutput [" + name() + "] commit failure on core [" + e.getKey() + "]", err);
				}
			});
		return docs.size();
	}

	@Override
	public void close() {
		ex.shutdown();
		Concurrents.waitShutdown(ex, logger);
		logger.debug("SolrOutput [" + name() + "] closing...");
		try {
			for (String core : cores)
				solr.commit(core);
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
}
