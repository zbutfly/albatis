package net.butfly.albatis.solr;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;

import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.async.Concurrents;

class SolrPersistFailover extends SolrFailover {
	private static final long serialVersionUID = -4766585003300311051L;
	private IBigQueue failover;

	public SolrPersistFailover(SolrOutput solr, String path, String solrUrl) throws IOException {
		super(solr);
		String qname = solrUrl.replace('/', '-');
		failover = new BigQueueImpl(IOs.mkdirs(path + "/" + solr.name()), qname);
		logger.info(MessageFormat.format("SolrOutput [{0}] failover [persist mode] init: [{1}/{2}] with name [{3}], init size [{4}].", solr
				.name(), path, solr.name(), qname, failover.size()));
	}

	@Override
	protected void failover() {
		while (failover.isEmpty())
			Concurrents.waitSleep(1000);
		Map<String, List<SolrInputDocument>> fails = new HashMap<>();
		while (!failover.isEmpty() && !solr.closed.get()) {
			byte[] buf;
			try {
				buf = failover.dequeue();
			} catch (IOException e) {
				continue;
			}
			if (null == buf) return;
			SolrMessage<SolrInputDocument> sm = fromBytes(buf);
			if (null == sm) {
				logger.error(MessageFormat.format("SolrOutput [{0}] found invalid failover data, data lost.", solr.name()));
				continue;
			}
			List<SolrInputDocument> l = fails.computeIfAbsent(sm.getCore(), c -> new ArrayList<>(SolrOutput.DEFAULT_PACKAGE_SIZE));
			l.add(sm.getDoc());
			if (l.size() < SolrOutput.DEFAULT_PACKAGE_SIZE && !failover.isEmpty()) continue;
			l = fails.remove(sm.getCore());
			try {
				solr.solr.add(sm.getCore(), l, SolrOutput.DEFAULT_AUTO_COMMIT_MS);
				logger.debug(MessageFormat.format("SolrOutput [{0}] retried [{1}] successfully, failover remained: [{2}].", solr.name(), l
						.size(), failover.size()));
			} catch (SolrServerException | IOException e) {
				fail(sm.getCore(), l, e);
			}
		}
	}

	@Override
	public long size() {
		return failover.size();
	}

	@Override
	public boolean isEmpty() {
		return failover.isEmpty();
	}

	@Override
	public void fail(String core, List<SolrInputDocument> docs, Exception err) {
		for (SolrInputDocument doc : docs)
			try {
				failover.enqueue(toBytes(new SolrMessage<SolrInputDocument>(core, doc)));
			} catch (IOException e) {
				if (null != err) logger.error(MessageFormat.format("SolrOutput [{0}] failover full, [{1}] docs lost on [{2}]", solr.name(),
						docs.size(), core), err);
				else logger.error(MessageFormat.format("SolrOutput [{0}] failover full, [{1}] docs lost on [{2}]", solr.name(), docs.size(),
						core), e);
				return;
			}
		if (null != err) logger.warn(MessageFormat.format(
				"SolrOutput [{0}] add failure on [{1}] with [{2}] docs, push to failover, now [{3}] failover on [{1}], failure for [{4}]",
				solr.name(), core, docs.size(), failover.size(), err.getMessage()));
	}

	@Override
	public void fail(String core, SolrInputDocument doc, Exception err) {
		try {
			failover.enqueue(toBytes(new SolrMessage<SolrInputDocument>(core, doc)));
			logger.warn(MessageFormat.format(
					"SolrOutput [{0}] add failure on [{1}] with [{2}] docs, push to failover, now [{3}] failover on [{1}], failure for [{4}]",
					solr.name(), core, 1, failover.size(), err.getMessage()));
		} catch (IOException e) {
			if (null != err) logger.error(MessageFormat.format("SolrOutput [{0}] failover full, [{1}] docs lost on [{2}]", solr.name(), 1,
					core), err);
			else logger.error(MessageFormat.format("SolrOutput [{0}] failover full, [{1}] docs lost on [{2}]", solr.name(), 1, core), e);
		}

	}

	private byte[] toBytes(SolrMessage<SolrInputDocument> message) {
		if (null == message) return null;
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(baos);) {
			oos.writeObject(message);
			return baos.toByteArray();
		} catch (IOException e) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	private SolrMessage<SolrInputDocument> fromBytes(byte[] bytes) {
		if (null == bytes) return null;
		try {
			return (SolrMessage<SolrInputDocument>) new ObjectInputStream(new ByteArrayInputStream(bytes)).readObject();
		} catch (ClassNotFoundException | IOException e) {
			return null;
		}
	}

	@Override
	public void close() {
		try {
			failover.gc();
			failover.close();
		} catch (IOException e) {
			logger.error(MessageFormat.format("SolrOutput [{0}] failover close failure", solr.name()), e);
		}
	}
}