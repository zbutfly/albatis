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

import net.butfly.albacore.io.Record;
import net.butfly.albacore.serder.BsonSerder;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.Pair;

public class ElasticMessage extends Record {
	private static final long serialVersionUID = -125189207796104302L;

	final boolean updating;
	private final boolean upsert;
	final String id;
	private transient Script script;

	public ElasticMessage(String index, String type, String id, Map<String, Object> doc) {
		this(index, type, id, doc, true, false);
	}

	public ElasticMessage(String index, String type, String id, Map<String, Object> doc, boolean upsert, boolean updateing) {
		super(assembly(index, type), doc);
		this.id = id;
		this.upsert = upsert;
		this.updating = updateing;
		this.script = null;
	}

	private static String assembly(String index, String type) {
		if (null == index) return type;
		if (null == type) return index + "/";
		return index + "/" + type;
	}

	private static Pair<String, String> dessembly(String table) {
		String[] it = table.split("/", 2);
		return new Pair<>(it.length == 2 ? it[0] : null, it.length == 2 ? it[1] : it[0]);
	}

	public ElasticMessage(String index, String type, String id, Script script, Map<String, Object> upsertDoc) {
		super(assembly(index, type), upsertDoc);
		this.id = id;
		this.script = script;
		this.upsert = upsertDoc != null;
		this.updating = true;
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
		IOs.writeBytes(os, id.getBytes(), new byte[] { //
				(byte) (upsert ? 1 : 0), //
				(byte) (updating ? 1 : 0), //
				(byte) (null != script ? 1 : 0) //
		});
		if (null != script) writeScript(os, script);
	}

	public static ElasticMessage fromBytes(byte[] b) {
		if (null == b) throw new IllegalArgumentException();
		try (ByteArrayInputStream bais = new ByteArrayInputStream(b)) {
			byte[] t = IOs.readBytes(bais);
			Pair<String, String> it = null == t ? null : dessembly(new String(t));
			Map<String, Object> map = BsonSerder.DEFAULT_MAP.der(IOs.readBytes(bais));
			byte[][] attrs = IOs.readBytesList(bais);
			String id = new String(attrs[2]);
			boolean upsert = attrs[3][0] == 1;
			boolean updating = attrs[3][1] == 1;
			boolean scripting = attrs[3][2] == 1;
			Script script = scripting ? readScript(bais) : null;
			return scripting ? new ElasticMessage(it.v1(), it.v2(), id, script, map)
					: new ElasticMessage(it.v1(), it.v2(), id, map, upsert, updating);
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

	public void type(String type) {
		Pair<String, String> p = dessembly(table).v2(type);
		table = assembly(p.v1(), p.v2());
	}
}
