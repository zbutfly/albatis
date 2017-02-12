package net.butfly.albatis.elastic;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.transport.RemoteTransportException;

import net.butfly.albacore.io.faliover.FailoverOutput;
import net.butfly.albacore.utils.async.Concurrents;
import scala.Tuple2;

public final class EsOutput extends FailoverOutput<ElasticMessage, ElasticMessage> {
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
	protected int write(String key, Collection<ElasticMessage> values) {
		// TODO: List<ElasticMessage> fails = new ArrayList<>();
		try {
			List<UpdateRequest> v = io.list(values, ElasticMessage::update);
			if (v.isEmpty()) return 0;
			BulkRequest req = new BulkRequest().add(v.toArray(new UpdateRequest[v.size()]));
			logger().trace("Bulk size: " + req.estimatedSizeInBytes());
			do {
				try {
					Map<Boolean, List<BulkItemResponse>> resps = io.collect(conn.client().bulk(req).actionGet(), Collectors.partitioningBy(
							r -> r.isFailed()));
					if (resps.get(Boolean.TRUE).isEmpty()) return resps.get(Boolean.FALSE).size();
					else throw resps.get(Boolean.TRUE).get(0).getFailure().getCause();
				} catch (EsRejectedExecutionException | VersionConflictEngineException e) {
					Concurrents.waitSleep();
				}
			} while (true);
		} catch (RemoteTransportException e) {
			while (e.getCause() != null && e.getCause() instanceof RemoteTransportException)
				e = (RemoteTransportException) e.getCause();
			if (null == e.getCause()) throw e;
			else if (e.getCause() instanceof RuntimeException) throw (RuntimeException) e.getCause();
			else throw new RuntimeException(e);
		} catch (RuntimeException e) {
			throw e;
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected Tuple2<String, ElasticMessage> parse(ElasticMessage e) {
		return new Tuple2<>(e.getIndex() + "/" + e.getType(), e);
	}

	@Override
	protected ElasticMessage unparse(String key, ElasticMessage value) {
		return value;
	}
}
