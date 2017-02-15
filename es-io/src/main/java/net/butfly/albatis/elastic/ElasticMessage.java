package net.butfly.albatis.elastic;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.Script;

import net.butfly.albacore.io.Message;

public class ElasticMessage extends Message<String, UpdateRequest, ElasticMessage> {
	private static final long serialVersionUID = -125189207796104302L;

	private final boolean upsert;// true;
	final String index;
	final String type;
	final String id;
	private transient Script script;
	private final Map<String, Object> doc;

	public ElasticMessage(String index, String type, String id, Map<String, Object> values) {
		this(index, type, id, values, true);
	}

	public ElasticMessage(String index, String type, String id, Map<String, Object> doc, boolean upsert) {
		super();
		this.index = index;
		this.type = type;
		this.id = id;
		this.doc = doc;
		this.upsert = upsert;
		this.script = null;
	}

	public ElasticMessage(String index, String type, String id, Script script, Map<String, Object> upsertDoc) {
		super();
		this.index = index;
		this.type = type;
		this.id = id;
		this.script = script;
		this.upsert = upsertDoc != null;
		this.doc = upsertDoc;
	}

	public ElasticMessage(String index, String type, String id, Script script) {
		this(index, type, id, script, null);
	}

	@Override
	public UpdateRequest forWrite() {
		UpdateRequest req = script != null ? new UpdateRequest().script(script) : new UpdateRequest().doc(doc);
		return req.index(index).type(type).id(id).retryOnConflict(5).docAsUpsert(upsert);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder().append("/").append(index).append("/").append(type).append("/").append(id);
		if (script == null) sb.append("(upsert:").append(upsert).append("):\n\tdocument:").append(doc);
		else sb.append(":\n\tscript: ").append(script);
		return sb.toString();
	}

	@Override
	public byte[] toBytes() {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(baos);) {
			oos.writeUTF(index);
			oos.writeUTF(type);
			oos.writeUTF(id);
			oos.writeBoolean(upsert);
			oos.writeObject(doc);
			boolean scrpiting = null != script;
			oos.writeBoolean(scrpiting);
			if (scrpiting) Elastics.writeScript(oos, script);
			return baos.toByteArray();
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public String partition() {
		return index + "/" + type;
	}

	@SuppressWarnings("unchecked")
	public ElasticMessage(byte[] bytes) {
		if (null == bytes) throw new IllegalArgumentException();
		try (ObjectInputStream oos = new ObjectInputStream(new ByteArrayInputStream(bytes));) {
			index = oos.readUTF();
			type = oos.readUTF();
			id = oos.readUTF();
			upsert = oos.readBoolean();
			doc = (Map<String, Object>) oos.readObject();
			boolean scrpiting = oos.readBoolean();
			script = scrpiting ? Elastics.readScript(oos) : null;
		} catch (IOException | ClassNotFoundException e) {
			throw new IllegalArgumentException(e);
		}
	}
}
