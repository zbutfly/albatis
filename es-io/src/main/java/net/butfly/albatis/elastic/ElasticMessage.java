package net.butfly.albatis.elastic;

import java.io.Serializable;
import java.util.Map;

import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;

import net.butfly.albacore.utils.collection.Maps;

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

	public ElasticMessage(String index, String type, String id, String nestedField, String nestedItemKey, String nestedItemKeyValue,
			Map<String, Object> params) {
		super();
		this.index = index;
		this.type = type;
		this.id = id;
		String sc = //
				"int i = 0;\n"//
						+ "for(;i<ctx.source." + nestedField + ".size();i++){\n"//
						+ "	if(ctx.source." + nestedField + "[i]." + nestedItemKey + "==values." + nestedItemKey + "){\n"//
						+ "		ctx.source." + nestedField + "[i]=values;break;\n"//
						+ "}}\n"//
						+ "if(i>=ctx.source." + nestedField + ".size()){\n"//
						+ "	ctx.source." + nestedField + ".add(values);\n"//
						+ "}";
		this.script = new Script(sc, ScriptType.INLINE, "groovy", Maps.of("values", params, "key", nestedItemKeyValue));

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
		if (script != null) return new UpdateRequest().index(index).type(type).id(id).script(script);
		else return new UpdateRequest().docAsUpsert(upsert).index(index).type(type).id(id).doc(values);
	}
}
