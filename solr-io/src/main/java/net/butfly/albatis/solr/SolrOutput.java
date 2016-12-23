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

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import net.butfly.albacore.io.MapOutput;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.lambda.ConverterPair;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.logger.Logger;
import scala.Tuple3;

public class SolrOutput extends MapOutput<String, SolrMessage<SolrInputDocument>> {
	private static final long serialVersionUID = -2897525987426418020L;
	private static final Logger logger = Logger.getLogger(SolrOutput.class);
	static final int DEFAULT_AUTO_COMMIT_MS = 30000;
	static final int DEFAULT_PACKAGE_SIZE = 500;
	private static final int DEFAULT_PARALLELISM = 5;
	final SolrClient solr;
	// final AtomicBoolean closed;

	private final Set<String> cores;
	private final LinkedBlockingQueue<Runnable> tasks;
	private final SolrFailover failover;
	private final List<SolrSender> senders;

	public SolrOutput(String name, String baseUrl) throws IOException {
		this(name, baseUrl, null);
	}

	public SolrOutput(String name, String baseUrl, String failoverPath) throws IOException {
		super(name, m -> m.getCore());
		logger.info("SolrOutput [" + name + "] from [" + baseUrl + "]");
		Tuple3<String, String, String[]> t;
		try {
			t = Solrs.parseSolrURL(baseUrl);
		} catch (SolrServerException | URISyntaxException e) {
			throw new IOException(e);
		}
		cores = new HashSet<>(Arrays.asList(t._3()));
		solr = Solrs.open(baseUrl);
		tasks = new LinkedBlockingQueue<>(DEFAULT_PARALLELISM);
		ConverterPair<String, List<SolrInputDocument>, Exception> adding = (core, docs) -> {
			try {
				solr.add(core, docs, DEFAULT_AUTO_COMMIT_MS);
				return null;
			} catch (Exception e) {
				return e;
			}
		};

		if (failoverPath == null) failover = new SolrFailoverMemory(name(), adding);
		else failover = new SolrFailoverPersist(name(), adding, failoverPath, baseUrl);

		senders = new ArrayList<>();
		for (int i = 0; i < DEFAULT_PARALLELISM; i++) {
			SolrSender s = new SolrSender(name, tasks, i);
			senders.add(s);
		}
	}

	@Override
	public boolean enqueue0(String core, SolrMessage<SolrInputDocument> doc) {
		cores.add(core);
		if (!opened()) {
			failover.fail(core, doc.getDoc(), null);
			return false;
		}
		try {
			tasks.put(() -> {
				try {
					solr.add(core, doc.getDoc(), DEFAULT_AUTO_COMMIT_MS);
				} catch (Exception err) {
					failover.fail(core, doc.getDoc(), err);
				}
			});
		} catch (InterruptedException e) {
			failover.fail(core, doc.getDoc(), null);
		}
		return true;
	}

	@Override
	public long enqueue(Converter<SolrMessage<SolrInputDocument>, String> keying, List<SolrMessage<SolrInputDocument>> docs) {
		Map<String, List<SolrInputDocument>> map = new HashMap<>();
		for (SolrMessage<SolrInputDocument> d : docs)
			map.computeIfAbsent(d.getCore(), core -> new ArrayList<>()).add(d.getDoc());
		cores.addAll(map.keySet());
		for (Entry<String, List<SolrInputDocument>> e : map.entrySet()) {
			boolean inserted = false;
			if (opened()) do {
				Runnable run = () -> {
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
				};
				inserted = tasks.offer(run);
			} while (opened() && !inserted);
			if (!inserted) failover.fail(e.getKey(), e.getValue(), null);
		}
		return docs.size();
	}

	@Override
	public void closing() {
		super.closing();
		for (SolrSender s : senders)
			s.close();
		failover.close();
		logger.debug("SolrOutput [" + name() + "] all processing thread closed normally");
		try {
			for (String core : cores)
				solr.commit(core, false, false);
		} catch (IOException | SolrServerException e) {
			logger.error("SolrOutput close failure", e);
		}
		Solrs.close(solr);
	}

	@Override
	public Set<String> keys() {
		return cores;
	}

	public long fails() {
		return failover.size();
	}

}
