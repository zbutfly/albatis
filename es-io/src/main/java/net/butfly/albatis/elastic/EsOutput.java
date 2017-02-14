package net.butfly.albatis.elastic;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.transport.RemoteTransportException;

import net.butfly.albacore.io.Streams;
import net.butfly.albacore.io.faliover.Failover.FailoverException;
import net.butfly.albacore.io.faliover.FailoverOutput;
import net.butfly.albacore.utils.Exceptions;
import scala.Tuple2;

public final class EsOutput extends FailoverOutput<ElasticMessage, ElasticMessage> {
	private final ElasticConnection conn;

	public EsOutput(String name, String esUri, String failoverPath) throws IOException {
		super(name, failoverPath, 100, 20);
		conn = new ElasticConnection(esUri);
		open();
	}

	public ElasticConnection getConnection() {
		return conn;
	}

	@Override
	protected void closeInternal() {
		conn.close();
	}

	@Override
	protected int write(String key, Collection<ElasticMessage> values) throws FailoverException {
		if (values.isEmpty()) return 0;
		Map<String, String> fails = new ConcurrentHashMap<>();
		List<ElasticMessage> retries = new ArrayList<>(values);
		do {
			BulkRequest req = new BulkRequest().add(io.list(retries, ElasticMessage::update));
			logger().trace("Bulk size: " + req.estimatedSizeInBytes());
			try {
				TransportClient tc = conn.client();
				ActionFuture<BulkResponse> f = tc.bulk(req);
				BulkResponse bulk = f.actionGet();
				Map<Boolean, List<BulkItemResponse>> resps = io.collect(bulk, Collectors.partitioningBy(r -> r.isFailed()));
				if (resps.get(Boolean.TRUE).isEmpty()) return resps.get(Boolean.FALSE).size();
				Set<String> succs = io.map(resps.get(Boolean.FALSE), r -> r.getId(), Collectors.toSet());
				Map<Boolean, List<BulkItemResponse>> retryMap = io.collect(Streams.of(resps.get(Boolean.TRUE)), Collectors.partitioningBy(
						r -> {
							@SuppressWarnings("unchecked")
							Class<Throwable> c = (Class<Throwable>) r.getFailure().getCause().getClass();
							return EsRejectedExecutionException.class.isAssignableFrom(c) || VersionConflictEngineException.class
									.isAssignableFrom(c);
						}));
				Map<String, String> failing = io.collect(retryMap.get(Boolean.FALSE), Collectors.toConcurrentMap(r -> r.getFailure()
						.getId(), this::wrapErrorMessage));
				retries = io.list(Streams.of(retries).filter(es -> !succs.contains(es.getId()) && !failing.containsKey(es.getId())));
				fails.putAll(failing);
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException();
			}
		} while (!retries.isEmpty());
		if (fails.isEmpty()) return values.size();
		else {
			throw new FailoverException(io.collect(Streams.of(values).filter(es -> fails.containsKey(es.getId())), Collectors.toMap(
					es -> es, es -> fails.get(es.getId()))));
		}
	}

	private String wrapErrorMessage(BulkItemResponse r) {
		return "Writing failed id [" + r.getId() + "] for: " + Exceptions.unwrap(r.getFailure().getCause()).getMessage();
	}

	@Override
	protected Tuple2<String, ElasticMessage> parse(ElasticMessage e) {
		return new Tuple2<>(e.getIndex() + "/" + e.getType(), e);
	}

	@Override
	protected ElasticMessage unparse(String key, ElasticMessage value) {
		return value;
	}

	@Override
	protected byte[] toBytes(String key, ElasticMessage value) throws IOException {
		if (null == key || null == value) throw new IOException("Data to be failover should not be null");

		try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(baos);) {
			oos.writeUTF(key);
			oos.writeUTF(value.getIndex());
			oos.writeUTF(value.getType());
			oos.writeUTF(value.getId());
			Script script = value.getScript();
			if (null != script) {
				oos.writeBoolean(true);
				oos.writeUTF(script.getScript());
				oos.writeUTF(script.getType().name());
				oos.writeUTF(script.getLang());
				oos.writeObject(script.getParams());
			} else {
				oos.writeBoolean(false);
				oos.writeBoolean(value.isUpsert());
				oos.writeObject(value.getValues());
			}
			return baos.toByteArray();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Tuple2<String, ElasticMessage> fromBytes(byte[] bytes) throws IOException {
		if (null == bytes) throw new IOException("Data to be failover should not be null");
		try (ObjectInputStream oos = new ObjectInputStream(new ByteArrayInputStream(bytes));) {
			String key = oos.readUTF();
			String index = oos.readUTF();
			String type = oos.readUTF();
			String id = oos.readUTF();
			if (oos.readBoolean()) {
				String script = oos.readUTF();
				ScriptType st = ScriptType.valueOf(oos.readUTF());
				String lang = oos.readUTF();
				return new Tuple2<>(key, new ElasticMessage(index, type, id, new Script(script, st, lang, (Map<String, Object>) oos
						.readObject())));
			} else {
				boolean upsert = oos.readBoolean();
				return new Tuple2<>(key, new ElasticMessage(index, type, id, (Map<String, Object>) oos.readObject(), upsert));
			}
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
	}

	static {
		Exceptions.unwrap(RemoteTransportException.class, "getCause");
	}
}
