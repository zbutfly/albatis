package net.butfly.albatis.solr;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.MessageFormat;
import java.util.List;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;

class SolrPersistFailover extends SolrFailover {
	private IBigQueue failover;

	public SolrPersistFailover(SolrOutput solr, String path, String solrUrl) throws IOException {
		super(solr);
		logger.info(MessageFormat.format("SolrOutput [{0}] failover in persist mode.", solr.name()));
		failover = new BigQueueImpl(path, solrUrl.replace('/', '-'));
	}

	@Override
	protected void failover() {
		if (!failover.isEmpty()) {
			byte[] buf;
			try {
				buf = failover.dequeue();
			} catch (IOException e) {
				return;
			}
			if (null == buf) return;
			SolrMessage<SolrInputDocument> sm = fromBytes(buf);
			if (null == sm) {
				logger.error(MessageFormat.format("SolrOutput [{0}] found invalid failover data, data lost.", solr.name()));
				return;
			}
			try {
				solr.solr.add(sm.getCore(), sm.getDoc(), SolrOutput.DEFAULT_AUTO_COMMIT_MS);
			} catch (SolrServerException | IOException e) {
				logger.warn(MessageFormat.format(
						"SolrOutput [{0}] retry failure on core [{1}] with [{2}] docs, push back to failover, now [{3}] failovers.", solr
								.name(), sm.getCore(), 1, failover.size()), e);
				try {
					failover.enqueue(buf);
				} catch (IOException ee) {
					logger.warn(MessageFormat.format("SolrOutput [{0}] failover push back failure on core [{1}].", solr.name(), sm
							.getCore()), ee);
				}
			}
		} else try {
			sleep(1000);
		} catch (InterruptedException e) {}
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
				logger.error(MessageFormat.format("SolrOutput [{0}] failover full, [{1}] docs lost for core [{2}] ", solr.name(), 1, core),
						err);
			}
	}

	@Override
	public void fail(String core, SolrInputDocument doc, Exception err) {
		try {
			failover.enqueue(toBytes(new SolrMessage<SolrInputDocument>(core, doc)));
		} catch (IOException e) {
			logger.error(MessageFormat.format("SolrOutput [{0}] failover full, [{1}] docs lost for core [{2}] ", solr.name(), 1, core),
					err);
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
}