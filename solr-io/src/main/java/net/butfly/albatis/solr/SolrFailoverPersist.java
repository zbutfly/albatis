package net.butfly.albatis.solr;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.solr.common.SolrInputDocument;

import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;

import net.butfly.albacore.lambda.ConverterPair;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.async.Concurrents;

class SolrFailoverPersist extends SolrFailover {
	private static final long serialVersionUID = -4766585003300311051L;
	private IBigQueue failover;

	public SolrFailoverPersist(String solrName, ConverterPair<String, List<SolrInputDocument>, Exception> adding, String path,
			String solrUrl) throws IOException {
		super(solrName, adding);
		String qname;
		try {
			qname = new URI(solrUrl).getAuthority().replaceAll("/", "-");
		} catch (URISyntaxException e) {
			qname = solrUrl.replaceAll("/", "-");
		}
		failover = new BigQueueImpl(IOs.mkdirs(path + "/" + solrName), qname);
		start();
		logger.info(MessageFormat.format("Failover [persist mode] init: [{0}/{1}] with name [{2}], init size [{3}].", //
				path, solrName, qname, size()));
	}

	@Override
	protected void failover() {
		while (opened() && failover.isEmpty())
			Concurrents.waitSleep(1000);
		Map<String, List<SolrInputDocument>> fails = new HashMap<>();
		while (opened() && !failover.isEmpty()) {
			byte[] buf;
			try {
				buf = failover.dequeue();
			} catch (IOException e) {
				continue;
			}
			if (null == buf) return;
			SolrMessage<SolrInputDocument> sm = fromBytes(buf);
			if (null == sm) {
				logger.error("invalid failover found and lost.");
				continue;
			}
			List<SolrInputDocument> l = fails.computeIfAbsent(sm.getCore(), c -> new ArrayList<>(SolrOutput.DEFAULT_PACKAGE_SIZE));
			l.add(sm.getDoc());
			if (l.size() < SolrOutput.DEFAULT_PACKAGE_SIZE && !failover.isEmpty()) continue;
			l = fails.remove(sm.getCore());
			stats(l);
			Exception e = adding.apply(sm.getCore(), l);
			if (null != e) fail(sm.getCore(), l, e);

		}
		for (String core : fails.keySet()) {
			List<SolrInputDocument> l = fails.remove(core);
			Exception e = adding.apply(core, l);
			if (null != e) fail(core, l, e);
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
	public int fail(String core, List<SolrInputDocument> docs, Exception err) {
		int c = 0;
		for (SolrInputDocument doc : docs)
			try {
				failover.enqueue(toBytes(new SolrMessage<SolrInputDocument>(core, doc)));
				c++;
			} catch (IOException e) {
				if (null != err) logger.error(MessageFormat.format("Failover failed, [{0}] docs lost on [{1}], original caused by [{2}]", //
						docs.size(), core, err.getMessage()), e);
				else logger.error(MessageFormat.format("Failover failed, [{0}] docs lost on [{1}]", docs.size(), core), e);
			}
		if (null != err) logger.warn(MessageFormat.format(
				"Failure added on [{0}] with [{1}] docs, now [{2}] failover on [{0}], caused by [{3}]", //
				core, docs.size(), size(), err.getMessage()));
		return c;
	}

	@Override
	public boolean fail(String core, SolrInputDocument doc, Exception err) {
		try {
			failover.enqueue(toBytes(new SolrMessage<SolrInputDocument>(core, doc)));
		} catch (IOException e) {
			if (null != err) logger.error(MessageFormat.format("Failover failed, [{0}] docs lost on [{1}], original caused by [{2}]", //
					1, core, err.getMessage()), e);
			else logger.error(MessageFormat.format("Failover failed, [{0}] docs lost on [{1}]", 1, core), e);
			return false;
		}
		if (null != err) logger.warn(MessageFormat.format(
				"Failure added on [{0}] with [{1}] docs, now [{2}] failover on [{0}], caused by [{3}]", //
				core, 1, size(), err.getMessage()));
		return true;
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
	public void closing() {
		super.closing();
		try {
			failover.gc();
		} catch (IOException e) {
			logger.error("Failover cleanup failure", e);
		}
		long size = size();
		try {
			failover.close();
		} catch (IOException e) {
			logger.error("Failover close failure", e);
		} finally {
			if (size > 0) logger.error("Failover closed and remained [" + size + "].");
		}
	}
}