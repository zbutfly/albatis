package net.butfly.albacore.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import net.butfly.albacore.lambda.ConverterPair;
import net.butfly.albacore.utils.IOs;

public class URIs {
	public enum Schema {
		FILE, CLASSPATH, HTTP, HTTPS, JDBC, ZOOKEEPER, MONGODB
	}

	public static <T> T parse(String uriString, ConverterPair<Schema, URISpec, T> constr) {
		URISpec uri = parse(uriString);
		return constr.apply(schema(uri), uri);
	}

	public static URISpec parse(String uriString, Schema... accept) {
		URISpec uri = new URISpec(uriString);
		if (accept == null || accept.length == 0) return uri;
		Schema schema = uri.getScheme() == null ? Schema.FILE : Schema.valueOf(uri.getScheme().toUpperCase());
		for (Schema s : accept)
			if (s.equals(schema)) return uri;
		throw new IllegalArgumentException("Not acceptable schema: " + schema);
	}

	public static Schema schema(URISpec uri) {
		return uri.getScheme() == null ? Schema.FILE : Schema.valueOf(uri.getScheme().toUpperCase());
	}

	public static Properties params(URISpec uri) {
		Properties p = new Properties();
		for (String param : uri.getQuery().split("&")) {
			String[] kv = param.split("=", 2);
			p.setProperty(kv[0], kv.length > 1 ? kv[1] : null);
		}
		return p;
	}

	public static InputStream open(URISpec uri) throws IOException {
		switch (schema(uri)) {
		case FILE:
			return IOs.loadJavaFile(uri.getPath());
		case CLASSPATH:
			return Thread.currentThread().getContextClassLoader().getResourceAsStream(uri.getPath());
		case HTTP:
		case HTTPS:
			return uri.toURI().toURL().openStream();
		default:
			return null;
		}

	}

	public static InputStream open(String uriString, Schema... accept) throws IOException {
		return open(parse(uriString, accept));
	}
}
