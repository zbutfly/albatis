package net.butfly.albatis.solr;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import net.butfly.albacore.io.Output;
import net.butfly.albacore.io.faliover.Failover;
import net.butfly.albacore.io.faliover.HeapFailover;
import net.butfly.albacore.io.faliover.OffHeapFailover;
import net.butfly.albacore.lambda.ConverterPair;
import net.butfly.albacore.utils.logger.Logger;
import scala.Tuple2;
import scala.Tuple3;

public class SolrOutput extends Output<SolrMessage<SolrInputDocument>> {
	private static final long serialVersionUID = -2897525987426418020L;
	private static final Logger logger = Logger.getLogger(SolrOutput.class);
	static final int DEFAULT_AUTO_COMMIT_MS = 30000;
	static final int DEFAULT_PACKAGE_SIZE = 500;
	private static final int DEFAULT_PARALLELISM = 5;
	final SolrClient solr;

	private final Set<String> cores;
	private final Failover<String, SolrInputDocument> failover;
	private final String defaultCore;

	public SolrOutput(String name, String baseUrl) throws IOException {
		this(name, baseUrl, null);
	}

	public SolrOutput(String name, String baseUrl, String failoverPath) throws IOException {
		super(name);
		logger.info("SolrOutput [" + name + "] from [" + baseUrl + "]");
		Tuple3<String, String, String[]> t;
		try {
			t = Solrs.parseSolrURL(baseUrl);
		} catch (SolrServerException | URISyntaxException e) {
			throw new IOException(e);
		}
		defaultCore = t._2();
		cores = new HashSet<>(Arrays.asList(t._3()));
		solr = Solrs.open(baseUrl);
		ConverterPair<String, List<SolrInputDocument>, Exception> adding = (core, docs) -> {
			try {
				solr.add(core, docs, DEFAULT_AUTO_COMMIT_MS);
				return null;
			} catch (Exception e) {
				return e;
			}
		};

		if (failoverPath == null) failover = new HeapFailover<>(name(), adding, DEFAULT_PACKAGE_SIZE, DEFAULT_PARALLELISM);
		else failover = new OffHeapFailover<String, SolrInputDocument>(name(), adding, failoverPath, calcName(baseUrl),
				DEFAULT_PACKAGE_SIZE, DEFAULT_PARALLELISM) {
			private static final long serialVersionUID = 7620077959670870367L;

			@Override
			protected byte[] toBytes(String core, SolrInputDocument doc) {
				if (null == core || null == doc) return null;
				try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(baos);) {
					oos.writeObject(new SolrMessage<SolrInputDocument>(core, doc));
					return baos.toByteArray();
				} catch (IOException e) {
					return null;
				}
			}

			@Override
			@SuppressWarnings("unchecked")
			protected Tuple2<String, SolrInputDocument> fromBytes(byte[] bytes) {
				if (null == bytes) return null;
				try {
					SolrMessage<SolrInputDocument> sm = (SolrMessage<SolrInputDocument>) new ObjectInputStream(new ByteArrayInputStream(
							bytes)).readObject();
					return new Tuple2<>(sm.getCore(), sm.getDoc());
				} catch (ClassNotFoundException | IOException e) {
					return null;
				}
			}
		};
	}

	@Override
	public boolean enqueue0(SolrMessage<SolrInputDocument> doc) {
		return enqueue(Arrays.asList(doc)) == 1;
	}

	@Override
	public long enqueue(List<SolrMessage<SolrInputDocument>> docs) {
		Map<String, List<SolrInputDocument>> map = new HashMap<>();
		for (SolrMessage<SolrInputDocument> d : docs)
			map.computeIfAbsent(d.getCore() == null ? defaultCore : d.getCore(), core -> new ArrayList<>()).add(d.getDoc());
		cores.addAll(map.keySet());
		failover.doWithFailover(map, (k, vs) -> {
			try {
				solr.add(k, vs);
			} catch (SolrServerException | IOException e) {
				throw new RuntimeException(e);
			}
		}, k -> {
			try {
				solr.commit(k, false, false, true);
			} catch (SolrServerException | IOException e) {
				throw new RuntimeException(e);
			}
		});
		return docs.size();
	}

	@Override
	public void closing() {
		super.closing();
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

	public long fails() {
		return failover.size();
	}

	private static String calcName(String solrUrl) {
		try {
			return new URI(solrUrl).getAuthority().replaceAll("/", "-");
		} catch (URISyntaxException e) {
			return solrUrl.replaceAll("/", "-");
		}
	}
}
