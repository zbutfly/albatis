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
		super(uri, "files", "files:json", "files:json:txt","files:txt","files:csv","files:json:ftp","files:txt:ftp","files:csv:ftp", "files:json:txt:ftp");
		String[] schemas = uri.getSchemas();
		if (schemas.length == 1){
			format = "json";
			ftp = "";
			ext = "." + format;
		}else {
			if("ftp".equals(schemas[schemas.length-1])){
				ftp = "ftp";
				if (schemas.length > 3){
					format = schemas[schemas.length-3]+":"+schemas[schemas.length-2];
					ext = "." + schemas[schemas.length-1];
				}else {
					format = schemas[schemas.length-2];
					ext = "." + format;
				}
			}else {
				ftp = "";
				if (schemas.length > 2){
					format = schemas[schemas.length-2]+":"+schemas[schemas.length-1];
					ext = "." + schemas[schemas.length-1];
				}else {
					format = schemas[schemas.length-1];
					ext = "." + format;
				}
			}
		}
		if (!uri.getPath().endsWith("/")) root = uri.getPath() + "/";
		else root = uri.getPath();
		this.uri = uri;
	}

	@Override
	public FileOutput outputRaw(TableDesc... table) throws IOException {
		this.tables = table;
		return new FileOutput(this);
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
			return Colls.list("files:","files:json","files:txt","files:csv","files:json:txt","files:json:ftp","files:txt:ftp","files:csv:ftp","files:json:txt:ftp");
		}
	}
}
