package net.butfly.albatis.elastic;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.MessageFormat;

import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;

import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;

public enum ScriptLang {
	GROOVY("groovy", "groovy"), JAVASCRIPT("javascript", "js");
	private static final Logger logger = Logger.getLogger(ScriptLang.class);

	private final String lang;
	// private final String template;
	private String ext;

	private ScriptLang(String lang, String ext) {
		this.lang = lang;
		this.ext = ext;
	}

	public String readTemplate(Class<?> loadClass, String templateName) {
		String content = read(loadClass, new StringBuilder().append("/").append(templateName).append(".").append(ext).toString());
		if (null == content) content = new StringBuilder().append("/").append(loadClass.getPackage().getName().replaceAll("\\.", "/"))
				.append("/").append(templateName).append(".").append(ext).toString();
		return content;
	}

	private String read(Class<?> loadClass, String filename) {
		StringBuilder content = new StringBuilder();
		try (InputStream is = loadClass.getResourceAsStream(filename); BufferedReader r = new BufferedReader(new InputStreamReader(is));) {
			String l;
			while ((l = r.readLine()) != null)
				content.append(l);// .append("\n");
		} catch (Throwable e) {
			return null;
		}
		String c = content.toString();
		logger.debug("ElasticSearch script template loaded from [classpath:" + filename + "], content: \n\t" + c);
		return c;
	}

	public Script construct(Object scriptParams, String template, Object... templateArgs) {
		return new Script(MessageFormat.format(template, templateArgs), ScriptType.INLINE, lang, Maps.of("scriptParams", scriptParams));
	}
}