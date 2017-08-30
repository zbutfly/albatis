package net.butfly.albatis.elastic;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import net.butfly.albacore.io.Message;
import net.butfly.albacore.serder.BsonSerder;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.Pair;

public class ElasticMessage extends Message {
	private static final long serialVersionUID = -125189207796104302L;
	private transient Script script;

	public ElasticMessage(String index, String type, String id, Map<String, Object> doc) {
		this(index, type, id, doc, Op.UPSERT);
	}

	public ElasticMessage(String index, String type, String id, Map<String, Object> doc, Op op) {
		super(null, id, doc);
		table(index, type);
		op(op);
		this.script = null;
	}

	public ElasticMessage(String index, String type, String id, Script script, Map<String, Object> upsertDoc) {
		super(null, id, upsertDoc);
		table(index, type);
		op(null == upsertDoc ? Op.UPDATE : Op.UPSERT);
		this.script = script;
	}

	@Override
	public ElasticMessage table(String table) {
		if (null == table) return table(null, null);
		else {
			String[] it = table.split("/");
			if (it.length == 1) return table(null, it[0]);
			else return table(it[0], it[1]);
		}
	}

	public ElasticMessage table(String index, String type) {
		if (null == index) return (ElasticMessage) super.table(type);
		else if (null == type) return (ElasticMessage) super.table(index + "/");
		else return (ElasticMessage) super.table(index + "/" + type);
	}

	private Pair<String, String> dessemble() {
		if (null == table || table.isEmpty()) return new Pair<>(null, null);
		String[] it = table.split("/", 2);
		for (int i = 0; i < it.length; i++)
			if (it[i].isEmpty()) it[i] = null;
		if (it.length == 2) return new Pair<>(it[0], it[1]);
		else return new Pair<>(null, it[0]);
	}

	public ElasticMessage(String index, String type, String id, Script script) {
		this(index, type, id, script, null);
	}

	public String index() {
		return dessemble().v1();
	}

	public String type() {
		return dessemble().v2();
	}

	public DocWriteRequest<?> forWrite() {
		Pair<String, String> it = dessemble();
		switch (op) {
		case UPDATE:
			UpdateRequest update = new UpdateRequest(it.v1(), it.v2(), key);
			if (script == null) update.doc(this).docAsUpsert(false);
			else update.script(script);
			return update;
		case UPSERT:
			UpdateRequest upsert = new UpdateRequest(it.v1(), it.v2(), key);
			if (script == null) upsert.doc(this).docAsUpsert(true);
			else {
				upsert.script(script);
				if (!isEmpty()) upsert.upsert(new IndexRequest(it.v1(), it.v2(), key).source(this));
			}
			return upsert;
		case INSERT:
			if (script != null) throw new IllegalArgumentException("Script should only in UpdateRequest");
			IndexRequest insert = new IndexRequest(it.v1(), it.v2(), key);
			insert.source(this);
			return insert;
		case DELETE:
			// throw new UnsupportedOperationException("Delete not supporting");
			return null;
		}
		return null;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(op.name()).append("@").append(table).append("/").append(key);
		if (script == null) sb.append("(upsert:").append("):\n\tdocument:").append(this);
		else sb.append(":\n\tscript: ").append(script);
		return sb.toString();
	}

	@Override
	protected void write(OutputStream os) throws IOException {
		super.write(os);
		boolean s = null == script;
		IOs.writeBytes(os, new byte[] { (byte) (s ? 0 : 1) });
		if (s) IOs.writeBytes(os, script.getType().name().getBytes(), script.getLang().getBytes(), script.getIdOrCode().getBytes(),
				BsonSerder.DEFAULT_MAP.ser(script.getParams()));
	}

	public ElasticMessage(byte[] b) throws IOException {
		this(new ByteArrayInputStream(b));
	}

	public ElasticMessage(InputStream is) throws IOException {
		super(is);
		if (0 != IOs.readBytes(is)[0]) {
			byte[][] attrs = IOs.readBytesList(is);
			this.script = new Script(ScriptType.valueOf(new String(attrs[0])), new String(attrs[1]), new String(attrs[2]),
					BsonSerder.DEFAULT_MAP.der(attrs[3]));
		}
	}
}
