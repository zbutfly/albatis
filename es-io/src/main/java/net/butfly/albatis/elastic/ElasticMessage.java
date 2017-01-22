package net.butfly.albatis.elastic;

import java.io.Serializable;
import java.util.Map;

import org.elasticsearch.action.update.UpdateRequest;

public class ElasticMessage implements Serializable {
	private static final long serialVersionUID = -125189207796104302L;
	private final Map<String, Object> values;

	private final boolean upsert;// true;
	private final String index;
	private final String type;
	private final String id;

	public ElasticMessage(String type, String id, Map<String, Object> values, String index) {
		this(type, id, values, index, true);
	}

	public ElasticMessage(String type, String id, Map<String, Object> values, String index, boolean upsert) {
		super();
		this.values = values;
		this.upsert = upsert;
		this.index = index;
		this.type = type;
		this.id = id;
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
		return new UpdateRequest().docAsUpsert(upsert).index(index).type(type).id(id).doc(values);
	}
}
