package net.butfly.albatis.elastic;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import net.butfly.albacore.io.Message;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.Pair;

public class ElasticMessage extends Message {
	private static final long serialVersionUID = -125189207796104302L;

	public final String id;
	private String index;
	final boolean updating;
	private final boolean upsert;
	private transient Script script;

	public ElasticMessage(String index, String type, String id, Map<String, Object> doc) {
		this(index, type, id, doc, true, false);
	}

	public ElasticMessage(String index, String type, String id, Map<String, Object> doc, boolean upsert, boolean updating) {
		super(type, doc);
		this.index = index;
		this.id = id;
		this.upsert = upsert;
		this.updating = updating;
		this.script = null;
	}

	public ElasticMessage(String index, String type, String id, Script script, Map<String, Object> upsertDoc) {
		super(type, upsertDoc);
		this.index = index;
		this.id = id;
		this.upsert = upsertDoc != null;
		this.updating = true;
		this.script = script;
	}

	@Override
	public String table() {
		return index + "/" + table;
	}

	@Override
	public Message table(String table) {
		if (null == table) return table(null, null);
		else {
			String[] it = table.split("/");
			if (it.length == 1) return table(null, it[0]);
			else return table(it[0], it[1]);
		}
	}

	public Message table(String index, String type) {
		super.table(type);
		this.index = index;
		return this;
	}

	private static Pair<String, String> dessembly(String table) {
		String[] it = table.split("/", 2);
		return new Pair<>(it.length == 2 ? it[0] : null, it.length == 2 ? it[1] : it[0]);
	}

	public ElasticMessage(String index, String type, String id, Script script) {
		this(index, type, id, script, null);
	}

	public DocWriteRequest<?> forWrite() {
		Pair<String, String> it = dessembly(table);
		if (updating) {
			UpdateRequest req = new UpdateRequest(it.v1(), it.v2(), id);
			if (script == null) req.doc(this).docAsUpsert(upsert);
			else {
				req.script(script);
				if (upsert && !isEmpty()) req.upsert(new IndexRequest(it.v1(), it.v2(), id).source(this));
			}
			return req;
		} else {
			if (script != null) throw new IllegalArgumentException("Script should only in UpdateRequest");
			IndexRequest req = new IndexRequest(it.v1(), it.v2(), id);
			req.source(this);
			return req;
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(updating ? "UpdateRequest" : "IndexRequest").append("@").append(table).append("/").append(id);
		if (script == null) sb.append("(upsert:").append(upsert).append("):\n\tdocument:").append(this);
		else sb.append(":\n\tscript: ").append(script);
		return sb.toString();
	}

	@Override
	protected void write(OutputStream os) throws IOException {
		super.write(os);
		IOs.writeBytes(os, index.getBytes(), id.getBytes(), new byte[] { //
				(byte) (upsert ? 1 : 0), //
				(byte) (updating ? 1 : 0), //
				(byte) (null != script ? 1 : 0) //
		});
		if (null != script) writeScript(os, script);
	}

	public static ElasticMessage fromBytes(byte[] b) {
		if (null == b) throw new IllegalArgumentException();
		try (ByteArrayInputStream bais = new ByteArrayInputStream(b)) {
			Message base = Message.fromBytes(bais);

			byte[][] attrs = IOs.readBytesList(bais);
			String index = new String(attrs[0]);
			String id = new String(attrs[1]);
			boolean upsert = attrs[2][0] == 1;
			boolean updating = attrs[2][1] == 1;
			boolean scripting = attrs[2][2] == 1;
			Script script = scripting ? readScript(bais) : null;
			return scripting ? new ElasticMessage(index, base.table(), id, script, base)
					: new ElasticMessage(index, base.table(), id, base, upsert, updating);
		} catch (IOException e) {
			return null;
		}
	}

	static void writeScript(OutputStream oos, Script script) throws IOException {
		IOs.writeBytes(oos, script.getType().name().getBytes(), script.getLang().getBytes(), script.getIdOrCode().getBytes());
		IOs.writeObj(oos, script.getParams());
	}

	static Script readScript(ByteArrayInputStream oos) throws IOException {
		byte[][] attrs = IOs.readBytesList(oos);
		Map<String, Object> param = IOs.readObj(oos);
		return new Script(ScriptType.valueOf(new String(attrs[0])), new String(attrs[1]), new String(attrs[2]), param);
	}
}
