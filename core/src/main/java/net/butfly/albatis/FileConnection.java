package net.butfly.albatis;

import java.io.IOException;
import java.util.List;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.ext.FileOutput;

public class FileConnection extends DataConnection<Connection> {
	public final String format;
	public final String root;
	public final String ext;

	public FileConnection(URISpec uri) throws IOException {
		super(uri, "file", "file:json");
		String[] schemas = uri.getSchemas();
		if (schemas.length > 1) format = schemas[1];
		else format = "json";
		root = FileConnection.path(uri);
		ext = "." + format;
	}

	@SuppressWarnings("unchecked")
	@Override
	public FileOutput outputRaw(TableDesc... table) throws IOException {
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

	public static class Driver implements net.butfly.albatis.Connection.Driver<FileConnection> {
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
