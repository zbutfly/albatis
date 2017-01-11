package net.butfly.albatis.elastic;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;

import net.butfly.albacore.io.Input;
import net.butfly.albacore.utils.logger.Logger;

public class ElasticInput extends Input<GetMappingsRequest> {
	private static final long serialVersionUID = -5666669099160512388L;
	protected static final Logger logger = Logger.getLogger(ElasticInput.class);
	private final ElasticConnection elastic;

	public ElasticInput(String connection) throws IOException {
		super("elastic-input-queue");
		elastic = new ElasticConnection(connection);
		// if (null != filter && filter.length == 1 &&
		// elastic.search(filter[0]).actionGet().status() != RestStatus.FOUND)
		// throw new RuntimeException();
	}

	@Override
	public void closing() {
		super.closing();
		try {
			elastic.close();
		} catch (IOException e) {
			logger.error("Close failure", e);
		}
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
	public GetMappingsRequest dequeue0() {
		return null;
	}

	@Override
	public List<GetMappingsRequest> dequeue(long batchSize) {
		return super.dequeue(batchSize);
	}
}
