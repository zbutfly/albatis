package net.butfly.albatis.elastic;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.text.MessageFormat;
import java.util.Map;

import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;

public class ElasticScript extends org.elasticsearch.script.Script implements Serializable {
	private static final long serialVersionUID = -2198364206131002839L;

	public ElasticScript(String script, ScriptType type, String lang, Map<String, ? extends Object> params) {
		super(script, type, lang, params);
	}

	public enum ScriptLang {
		GROOVY("groovy", "groovy"), JAVASCRIPT("javascript", "js");

		private final String lang;
		// private final String template;
		private String ext;

		private ScriptLang(String lang, String ext) {
			this.lang = lang;
			this.ext = ext;
		}

		public String read(Class<?> loadClass, String templateName) {
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
			return content.toString();
		}

		public Script load(Map<String, Object> scriptParams, String template, Object... templateArgs) {
			return new ElasticScript(MessageFormat.format(template, templateArgs), ScriptType.INLINE, lang, scriptParams);
		}
	}
}
