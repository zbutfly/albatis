package net.butfly.albatis.elastic;

import static net.butfly.albacore.paral.Sdream.of;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;

import net.butfly.albacore.paral.Task;
import net.butfly.albacore.utils.Exceptions;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.io.Rmap;

public class ElasticOutputAsync extends ElasticOutput {
	private static final long serialVersionUID = -5126312703438177937L;

	public ElasticOutputAsync(String name, ElasticConnection conn) throws IOException {
		super(name, conn);
	}

	@Override
	protected final void go(Map<Object, Rmap> remains) {
		while (!remains.isEmpty()) {
			List<DocWriteRequest<?>> reqs = Colls.list();
			remains.values().forEach(r -> reqs.add(Elastics.forWrite(r)));
			if (reqs.isEmpty()) return;
			try {
				conn.client.bulk(new BulkRequest().add(reqs), new EnqueueListener(remains));
			} catch (IllegalStateException ex) {
				logger().error("Elastic client fail: [" + ex.toString() + "]");
			}
		}
	}

	protected final class EnqueueListener implements ActionListener<BulkResponse> {
		private final Map<Object, Rmap> remains;

		private EnqueueListener(Map<Object, Rmap> remains) {
			this.remains = remains;
		}

		public void join() {
			while (!remains.isEmpty()) Task.waitSleep(10);
		}

		@Override
		public void onResponse(BulkResponse response) {
			process(remains, response);
		}

		@Override
		public void onFailure(Exception e) {
			logger.error("INFO~~~~>ElasticOutput response error: " + e.toString());
			Throwable t = Exceptions.unwrap(e);
			if (noRetry(t)) logger().warn("Elastic connection op failed [" + t + "], [" + remains.size() + "] fails", t);
			else failed(of(remains.values()));
		}
	}
}
