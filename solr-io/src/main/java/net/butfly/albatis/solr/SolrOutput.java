package net.butfly.albatis.solr;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import net.butfly.albacore.io.Output;
import net.butfly.albacore.io.faliover.Failover;
import net.butfly.albacore.io.faliover.HeapFailover;
import net.butfly.albacore.io.faliover.OffHeapFailover;
import net.butfly.albacore.lambda.ConverterPair;
import net.butfly.albacore.utils.logger.Logger;
import scala.Tuple2;

public class SolrOutput extends Output<SolrMessage<SolrInputDocument>> {
	private static final long serialVersionUID = -2897525987426418020L;
	private static final Logger logger = Logger.getLogger(SolrOutput.class);
	private static final int DEFAULT_PARALLELISM = 5;
	static final int DEFAULT_AUTO_COMMIT_MS = 30000;
	static final int DEFAULT_PACKAGE_SIZE = 500;

	private final ConverterPair<String, List<SolrInputDocument>, Exception> writing;
	private final SolrConnection solr;

	private final Failover<String, SolrInputDocument> failover;
	private Consumer<String> committing;

	public SolrOutput(String name, String baseUrl) throws IOException {
		this(name, baseUrl, null);
	}

	public SolrOutput(String name, String baseUrl, String failoverPath) throws IOException {
		super(name);
		logger.info("[" + name + "] from [" + baseUrl + "]");
		solr = new SolrConnection(baseUrl);
		writing = (core, docs) -> {
			try {
				solr.client().add(core, docs, DEFAULT_AUTO_COMMIT_MS);
				return null;
			} catch (Exception e) {
				return e;
			}
		};
		committing = k -> {
			try {
				solr.client().commit(k, false, false, true);
			} catch (SolrServerException | IOException e) {
				throw new RuntimeException(e);
			}
		};

		if (failoverPath == null) failover = new HeapFailover<>(name(), writing, committing, DEFAULT_PACKAGE_SIZE, DEFAULT_PARALLELISM);
		else failover = new OffHeapFailover<String, SolrInputDocument>(name(), writing, committing, failoverPath, solr.getURI().getHost()
				.replaceAll("[:,]", "_"), DEFAULT_PACKAGE_SIZE, DEFAULT_PARALLELISM) {
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
			map.computeIfAbsent(d.getCore() == null ? solr.getDefaultCore() : d.getCore(), core -> new ArrayList<>()).add(d.getDoc());
		failover.dotry(map);
		return docs.size();
	}

	@Override
	public void close() {
		super.close();
		failover.close();
		closeSolr();
	}

	private void closeSolr() {
		try {
			for (String core : solr.getCores())
				solr.client().commit(core, false, false);
		} catch (IOException | SolrServerException e) {
			logger.error("Close failure", e);
		}
		try {
			solr.close();
		} catch (IOException e) {
			logger.error("Close failure", e);
		}
	}

	public long fails() {
		return failover.size();
	}
}
