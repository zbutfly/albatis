package net.butfly.albatis.elastic;

import java.io.Serializable;
import java.util.Map;

import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.Script;

public class ElasticMessage implements Serializable {
	private static final long serialVersionUID = -125189207796104302L;
	private final Map<String, Object> values;

	private final boolean upsert;// true;
	private final String index;
	private final String type;
	private final String id;
	private final Script script;

	public ElasticMessage(String index, String type, String id, Map<String, Object> values) {
		this(index, type, id, values, true);
	}

	public ElasticMessage(String index, String type, String id, Map<String, Object> values, boolean upsert) {
		super();
		this.index = index;
		this.type = type;
		this.id = id;
		this.values = values;
		this.upsert = upsert;
		this.script = null;
	}

	public ElasticMessage(String index, String type, String id, Script script) {
		super();
		this.index = index;
		this.type = type;
		this.id = id;
		this.script = script;
		this.values = null;
		this.upsert = false;
	}

	public Map<String, Object> getValues() {
		return values;
	}

	public String getId() {
		return id;
	}

	public boolean isUpsert() {
		return upsert;
	}

	public String getIndex() {
		return index;
	}

	public String getType() {
		return type;
	}

	public UpdateRequest update() {
		UpdateRequest req = script != null ? new UpdateRequest().script(script) : new UpdateRequest().docAsUpsert(upsert).doc(values);
		return req.index(index).type(type).id(id).retryOnConflict(5);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder().append("/").append(index).append("/").append(type).append("/").append(id);
		if (script == null) sb.append("(upsert:").append(upsert).append("):\n\tdocument:").append(values);
		else sb.append(":\n\tscript: ").append(script);
		return sb.toString();
	}
}
