package net.butfly.albatis.elastic;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.text.MessageFormat;
import java.util.Map;

import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.ScriptService.ScriptType;

import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;

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

	private static final Logger logger = Logger.getLogger(ScriptLang.class);

	public enum ScriptLang {
		GROOVY("groovy", "groovy"), JAVASCRIPT("javascript", "js");

		public final String lang;
		public final String template;

		private ScriptLang(String lang, String ext) {
			this.lang = lang;
			StringBuilder content = new StringBuilder();
			String tempFile = "/" + ElasticMessage.class.getPackage().getName().replaceAll("\\.", "/") + "/template." + ext;
			try (InputStream is = this.getClass().getResourceAsStream(tempFile);
					BufferedReader r = new BufferedReader(new InputStreamReader(is));) {
				String l;
				while ((l = r.readLine()) != null)
					content.append(l);// .append("\n");
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			template = content.toString();
			logger.debug("ElasticMessage script template for lang [" + lang + "]: \n" + template);
		}
	}

	private class Script extends org.elasticsearch.script.Script implements Serializable {
		private static final long serialVersionUID = -2198364206131002839L;

		public Script(String script, ScriptType type, String lang, Map<String, ? extends Object> params) {
			super(script, type, lang, params);
		}
	}

	public ElasticMessage(String index, String type, String id, ScriptLang lang, String nestedField, String nestedItemKey,
			Map<String, Object> params) {
		super();
		this.index = index;
		this.type = type;
		this.id = id;
		this.script = new Script(MessageFormat.format(lang.template, nestedField, nestedItemKey), ScriptType.INLINE, lang.lang, Maps.of(
				"values", params));
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
}
