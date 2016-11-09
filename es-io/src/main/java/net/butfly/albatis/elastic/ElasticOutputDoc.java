package net.butfly.albatis.elastic;

import java.io.Serializable;
import java.util.Map;

public class ElasticOutputDoc implements Serializable {
	private static final long serialVersionUID = -125189207796104302L;
	private final Map<String, Object> values;

	private final boolean upsert;// true;
	private final String index;
	private final String type;
	private final String id;

	public ElasticOutputDoc(String type, String id, Map<String, Object> values, String index) {
		this(type, id, values, index, true);
	}

	public ElasticOutputDoc(String type, String id, Map<String, Object> values, String index, boolean upsert) {
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
}
