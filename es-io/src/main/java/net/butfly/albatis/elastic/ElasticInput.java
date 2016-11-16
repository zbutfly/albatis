package net.butfly.albatis.elastic;

import java.net.UnknownHostException;
import java.util.List;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.client.transport.TransportClient;

import net.butfly.albacore.io.InputQueueImpl;
import net.butfly.albacore.utils.logger.Logger;

public class ElasticInput extends InputQueueImpl<GetMappingsRequest> {
	private static final long serialVersionUID = -5666669099160512388L;
	protected static final Logger logger = Logger.getLogger(ElasticInput.class);
	private final TransportClient elastic;

	public ElasticInput(String clusterName, String... hostports) throws UnknownHostException {
		super("elastic-input-queue");
		elastic = Elastics.connect(clusterName, hostports);
		// if (null != filter && filter.length == 1 &&
		// elastic.search(filter[0]).actionGet().status() != RestStatus.FOUND)
		// throw new RuntimeException();
	}

	@Override
	public void close() {
		elastic.close();
	}

	@Override
	public long size() {
		return 0;
	}

	@Override
	public boolean empty() {
		return true;
	}

	@Override
	protected GetMappingsRequest dequeueRaw() {
		return null;
	}

	@Override
	public GetMappingsRequest dequeue() {
		return super.dequeue();
	}

	@Override
	public List<GetMappingsRequest> dequeue(long batchSize) {
		return super.dequeue(batchSize);
	}
}