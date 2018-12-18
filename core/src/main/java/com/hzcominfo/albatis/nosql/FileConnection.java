package com.hzcominfo.albatis.nosql;

import java.io.IOException;
import java.util.List;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.ext.FileOutput;

public class FileConnection extends DataConnection<Connection> {
	public final String format;
	public final String root;
	public final String ext;

	public FileConnection(URISpec uri) throws IOException {
		super(uri, "file", "file:json");
		String[] schemas = uri.getScheme().split(":");
		if (schemas.length > 1) format = schemas[1];
		else format = "json";
		root = FileConnection.path(uri);
		ext = "." + format;
	}

	@Override
	public <M extends Rmap> Input<M> createInput(TableDesc... table) throws IOException {
		return null;
	}

	@Override
	public FileOutput createOutput(TableDesc... table) throws IOException {
		return new FileOutput(this);
	}

	public static String path(URISpec uri) {
		String host = uri.getHost();
		String path = uri.getPath();
		if (!path.endsWith("/")) path += "/";
		switch (host) {
		case ".":
		case "~":
			path = host + path;
			break;
		case "":
			break;
		default:
			throw new UnsupportedOperationException("hosts can only be '.', '~' or empty.");
			// return parseHdfs();
		}
		return path;
	}

	public static class Driver implements com.hzcominfo.albatis.nosql.Connection.Driver<FileConnection> {
		static {
			DriverManager.register(new Driver());
		}

		@Override
		public FileConnection connect(URISpec uriSpec) throws IOException {
			return new FileConnection(uriSpec);
		}

		@Override
		public List<String> schemas() {
			return Colls.list("file");
		}
	}
}
