package net.butfly.albatis.elastic;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import net.butfly.albacore.io.Message;
import net.butfly.albacore.utils.IOs;

public class ElasticMessage extends Message<String, DocWriteRequest<?>, ElasticMessage> {
	private static final long serialVersionUID = -125189207796104302L;

	final boolean updating;
	private final boolean upsert;
	final String index;
	String type;
	final String id;
	private transient Script script;
	private final Map<String, Object> doc;

	public ElasticMessage(String index, String type, String id, Map<String, Object> doc) {
		this(index, type, id, doc, true, false);
	}

	public ElasticMessage(String index, String type, String id, Map<String, Object> doc, boolean upsert, boolean updateing) {
		super();
		this.index = index;
		this.type = type;
		this.id = id;
		this.doc = doc;
		this.upsert = upsert;
		this.updating = updateing;
		this.script = null;
	}

	public ElasticMessage(String index, String type, String id, Script script, Map<String, Object> upsertDoc) {
		super();
		this.index = index;
		this.type = type;
		this.id = id;
		this.script = script;
		this.upsert = upsertDoc != null;
		this.updating = true;
		this.doc = upsertDoc;
	}

	public ElasticMessage(String index, String type, String id, Script script) {
		this(index, type, id, script, null);
	}

	@Override
	public DocWriteRequest<?> forWrite() {
		if (updating) {
			UpdateRequest req = new UpdateRequest(index, type, id);
			if (script == null) req.doc(doc).docAsUpsert(upsert);
			else {
				req.script(script);
				if (upsert && doc != null && !doc.isEmpty()) req.upsert(new IndexRequest(index, type, id).source(doc));
			}
			return req;
		} else {
			if (script != null) throw new IllegalArgumentException("Script should only in UpdateRequest");
			IndexRequest req = new IndexRequest(index, type, id);
			req.source(doc);
			return req;
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(updating ? "UpdateRequest" : "IndexRequest").append("@").append(index).append("/").append(type)
				.append("/").append(id);
		if (script == null) sb.append("(upsert:").append(upsert).append("):\n\tdocument:").append(doc);
		else sb.append(":\n\tscript: ").append(script);
		return sb.toString();
	}

	@Override
	public String partition() {
		return index + "/" + type;
	}

	@Override
	public byte[] toBytes() {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();) {
			IOs.writeBytes(baos, index.getBytes(), type.getBytes(), id.getBytes(), //
					new byte[] { (byte) (upsert ? 1 : 0), (byte) (updating ? 1 : 0), (byte) (null != script ? 1 : 0) });
			IOs.writeObj(baos, doc);
			if (null != script) writeScript(baos, script);
			return baos.toByteArray();
		} catch (IOException e) {
			return null;
		}
	}

	public ElasticMessage(byte[] bytes) {
		if (null == bytes) throw new IllegalArgumentException();
		try (ByteArrayInputStream bios = new ByteArrayInputStream(bytes);) {
			byte[][] attrs = IOs.readBytesList(bios);
			index = new String(attrs[0]);
			type = new String(attrs[1]);
			id = new String(attrs[2]);
			doc = IOs.readObj(bios);
			upsert = attrs[3][0] == 1;
			updating = attrs[3][1] == 1;
			boolean scripting = attrs[3][2] == 1;
			script = scripting ? readScript(bios) : null;
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	static void writeScript(ByteArrayOutputStream oos, Script script) throws IOException {
		IOs.writeBytes(oos, script.getType().name().getBytes(), script.getLang().getBytes(), script.getIdOrCode().getBytes());
		IOs.writeObj(oos, script.getParams());
	}

	static Script readScript(ByteArrayInputStream oos) throws IOException {
		byte[][] attrs = IOs.readBytesList(oos);
		Map<String, Object> param = IOs.readObj(oos);
		return new Script(ScriptType.valueOf(new String(attrs[0])), new String(attrs[1]), new String(attrs[2]), param);
	}
}
