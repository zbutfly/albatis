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
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.Message;

public class ElasticOutputAsync extends ElasticOutput {
	private static final Logger logger = Logger.getLogger(ElasticOutputAsync.class);
	public static final int MAX_RETRY = 3;
	public static final int SUGGEST_BATCH_SIZE = 1000;

	public ElasticOutputAsync(String name, ElasticConnection conn) throws IOException {
		super(name, conn);
	}

	@Override
	protected final void go(Map<String, Message> remains) {
		while (!remains.isEmpty()) {
			@SuppressWarnings("rawtypes")
			List<DocWriteRequest> reqs = of(remains.values()).map(Elastics::forWrite).list();
			if (reqs.isEmpty()) return;
			try {
				conn.client().bulk(new BulkRequest().add(reqs), new EnqueueListener(remains));
			} catch (IllegalStateException ex) {
				logger().error("Elastic client fail: [" + ex.toString() + "]");
			}
		}
	}

	protected final class EnqueueListener implements ActionListener<BulkResponse> {
		private final Map<String, Message> remains;

		private EnqueueListener(Map<String, Message> remains) {
			this.remains = remains;
		}

		public void join() {
			while (!remains.isEmpty())
				Task.waitSleep(10);
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
