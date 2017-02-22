package net.butfly.albatis.elastic;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.Script;

import net.butfly.albacore.io.Message;

public class ElasticMessage extends Message<String, ActionRequest<?>, ElasticMessage> {
	private static final long serialVersionUID = -125189207796104302L;

	final boolean updating;
	private final boolean upsert;
	final String index;
	final String type;
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
		this.updating = false;
		this.doc = upsertDoc;
	}

	public ElasticMessage(String index, String type, String id, Script script) {
		this(index, type, id, script, null);
	}

	@Override
	public ActionRequest<?> forWrite() {
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
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(baos);) {
			oos.writeUTF(index);
			oos.writeUTF(type);
			oos.writeUTF(id);
			oos.writeBoolean(upsert);
			oos.writeBoolean(updating);
			oos.writeObject(doc);
			boolean scrpiting = null != script;
			oos.writeBoolean(scrpiting);
			if (scrpiting) Elastics.writeScript(oos, script);
			return baos.toByteArray();
		} catch (IOException e) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public ElasticMessage(byte[] bytes) {
		if (null == bytes) throw new IllegalArgumentException();
		try (ObjectInputStream oos = new ObjectInputStream(new ByteArrayInputStream(bytes));) {
			index = oos.readUTF();
			type = oos.readUTF();
			id = oos.readUTF();
			upsert = oos.readBoolean();
			updating = oos.readBoolean();
			doc = (Map<String, Object>) oos.readObject();
			boolean scrpiting = oos.readBoolean();
			script = scrpiting ? Elastics.readScript(oos) : null;
		} catch (IOException | ClassNotFoundException e) {
			throw new IllegalArgumentException(e);
		}
	}
}
