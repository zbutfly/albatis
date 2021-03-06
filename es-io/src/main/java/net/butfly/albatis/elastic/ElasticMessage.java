package net.butfly.albatis.elastic;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import net.butfly.albacore.serder.BsonSerder;
import net.butfly.albacore.utils.IOs;
import net.butfly.albatis.io.Message;

public class ElasticMessage extends Message {
	private static final long serialVersionUID = -125189207796104302L;
	transient Script script;

	public ElasticMessage(String index, String type, String id, Script script, Map<String, Object> upsertDoc) {
		super(null, id, upsertDoc);
		table(index + "/" + type);
		op(null == upsertDoc ? Op.UPDATE : Op.UPSERT);
		this.script = script;
	}

	public ElasticMessage(String index, String type, String id, Script script) {
		this(index, type, id, script, null);
	}

	public String index() {
		return Elastics.dessemble(table).v1();
	}

	public String type() {
		return Elastics.dessemble(table).v2();
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
				BsonSerder.map(script.getParams()));
	}

	public ElasticMessage(byte[] b) throws IOException {
		this(new ByteArrayInputStream(b));
	}

	public ElasticMessage(InputStream is) throws IOException {
		super(is);
		if (0 != IOs.readBytes(is)[0]) {
			byte[][] attrs = IOs.readBytesList(is);
			this.script = new Script(ScriptType.valueOf(new String(attrs[0])), new String(attrs[1]), new String(attrs[2]), BsonSerder.map(
					attrs[3]));
		}
	}
}
