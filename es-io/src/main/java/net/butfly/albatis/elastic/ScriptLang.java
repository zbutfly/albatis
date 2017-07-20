package net.butfly.albatis.elastic;

import java.io.InputStream;
import java.text.MessageFormat;

import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import com.google.common.base.Joiner;

import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.Reflections;
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
		if (null == content) content = new StringBuilder().append("/").append(Reflections.packageName(loadClass).replaceAll("\\.", "/"))
				.append("/").append(templateName).append(".").append(ext).toString();
		return content;
	}

	private String read(Class<?> loadClass, String filename) {
		String content;
		try (InputStream is = loadClass.getResourceAsStream(filename);) {
			content = Joiner.on("\n").join(IOs.readLines(is));
		} catch (Throwable e) {
			return null;
		}
		logger.debug("ElasticSearch script template loaded from [classpath:" + filename + "], content: \n\t" + content);
		return content;
	}

	public Script construct(Object scriptParams, String template, Object... templateArgs) {
		return new Script(ScriptType.INLINE, lang, MessageFormat.format(template, templateArgs), Maps.of("__scriptParams", scriptParams));
	}
}