package net.butfly.albatis.elastic;

import static net.butfly.albatis.io.Rmap.Op.DELETE;
import static net.butfly.albatis.io.Rmap.Op.INSERT;
import static net.butfly.albatis.io.Rmap.Op.UPDATE;
import static net.butfly.albatis.io.Rmap.Op.UPSERT;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;

import net.butfly.albacore.utils.Pair;
import net.butfly.albatis.io.Rmap;

public class Elastics {
	public static DocWriteRequest<?> forWrite(Rmap m) {
		Pair<String, String> it = dessemble(m.table());
		if (null == m.key()) return null;
		switch (m.op()) {
		case UPDATE:
			UpdateRequest update = new UpdateRequest(it.v1(), it.v2(), m.key().toString());
			update.doc(m).docAsUpsert(false);
			return update;
		case UPSERT:
			UpdateRequest upsert = new UpdateRequest(it.v1(), it.v2(), m.key().toString());
			upsert.doc(m).docAsUpsert(true);
			return upsert;
		case INSERT:
			return new IndexRequest(it.v1(), it.v2(), m.key().toString()).source(m);
		case DELETE:
			return new DeleteRequest(it.v1(), it.v2(), m.key().toString());
		default:
			return null;
		}
	}

	public static DocWriteRequest<?> forScriptWrite(ElasticMessage m) {
		if (m.script == null) return forWrite((Rmap) m);
		Pair<String, String> it = dessemble(m.table());
		if (null == m.key()) return null;
		switch (m.op()) {
		case UPDATE:
			UpdateRequest update = new UpdateRequest(it.v1(), it.v2(), m.key().toString());
			update.script(m.script);
			return update;
		case UPSERT:
			UpdateRequest upsert = new UpdateRequest(it.v1(), it.v2(), m.key().toString());
			upsert.script(m.script);
			if (!m.isEmpty()) upsert.upsert(new IndexRequest(it.v1(), it.v2(), m.key().toString()).source(m));
			return upsert;
		case INSERT:
			throw new IllegalArgumentException("Script should only in UpdateRequest");
		case DELETE:
			return new DeleteRequest(it.v1(), it.v2(), m.key().toString());
		default:
			return null;
		}
	}

	public static Pair<String, String> dessemble(String indexAndType) {
		if (null == indexAndType || indexAndType.isEmpty()) return new Pair<>(null, null);
		String[] it = indexAndType.split("/", 2);
		for (int i = 0; i < it.length; i++)
			if (it[i].isEmpty()) it[i] = null;
		if (it.length == 2) return new Pair<>(it[0], it[1]);
		else return new Pair<>(null, it[0]);
	}

	public static String assembly(String index, String type) {
		if (index == null) return type;
		if (type == null) return index + "/";
		return index + "/" + type;
	}
}
