package net.butfly.albatis.file;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.Connection;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.TableDesc;

import java.io.IOException;
import java.util.List;

public class FileConnection extends DataConnection<Connection> {
	public final String format;
	public final String root;
	public final String ext;
	public TableDesc[] tables;
	public final String ftp;
	public final URISpec uri;

	public FileConnection(URISpec uri) throws IOException {
		super(uri, "files", "files:json","files:txt","files:csv","files:txt:ftp","files:csv:ftp");
		String[] schemas = uri.getSchemas();
		if (schemas.length > 1) format = schemas[1];
		else format = "json";
		if (schemas.length > 2) ftp = schemas[2];
		else ftp = "";
		root = uri.getPath();
		if ("json".equals(format)) ext = ".txt";
		else ext = "." + format;
		this.uri = uri;
	}

	@SuppressWarnings("unchecked")
	@Override
	public FileOutput outputRaw(TableDesc... table) throws IOException {
		this.tables = table;
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
			return Colls.list("files:","files:json","files:txt","files:csv","files:txt:ftp","files:csv:ftp");
		}
	}
}
