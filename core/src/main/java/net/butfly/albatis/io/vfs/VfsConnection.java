package net.butfly.albatis.io.vfs;

import static net.butfly.albacore.utils.collection.Colls.empty;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.Connection;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.TableDesc;

public class VfsConnection extends DataConnection<Connection> {
	static final Logger logger = Logger.getLogger(VfsConnection.class);
	private static FileSystemManager FS_MANAGER = null;
	private final URISpec vfsUri;
	final String directory;
	final String prefix;
	final String suffix;
	final long limit;
	final String ext;

	// private final FileObject dirobj;
	// private final String filename;

	public VfsConnection(URISpec uri) throws IOException {
		super(uri, SCHEMAS);
		String[] schemas = uri.getSchemas();
		if ("file".equals(schemas[0]) || "vfs".equals(schemas[0])) schemas = Arrays.copyOfRange(schemas, 1, schemas.length);
		if (0 == schemas.length) schemas = new String[] { "file" };
		vfsUri = uri.schema(schemas[0]);
		if (null == FS_MANAGER) FS_MANAGER = init();
		prefix = vfsUri.fetchParameter("px");
		String s = vfsUri.fetchParameter("sx");
		limit = Long.parseLong(vfsUri.fetchParameter("limit", "5000"));
		suffix = limit > 0 && null == prefix && null == s ? "yyyyMMddhhmmssSSS" : s;
		String full = vfsUri.toString();
		int pos = full.lastIndexOf("/");
		s = full.substring(0, pos);
		directory = s.endsWith("/") ? s : (s + "/");
		// dirobj = FS_MANAGER.resolveFile(directory);
		// if (!dirobj.exists()) dirobj.createFolder();
		s = full.substring(pos + 1);

		if (schemas.length > 1) ext = schemas[1];
		else if (empty(s)) ext = "json";
		else ext = (pos = s.lastIndexOf('.')) < 0 ? "json" : empty(s = s.substring(pos + 1)) ? "json" : s;
		// filename = empty(s) ? null : s;
	}

	@SuppressWarnings("unchecked")
	@Override
	public VfsOutput outputRaw(TableDesc... table) throws IOException {
		return new VfsOutput(this);
	}

	public FileObject open(String realtivePath) throws FileSystemException {
		String path = directory + realtivePath;
		// int pos = path.lastIndexOf("/");
		// String dir = path.substring(0, pos);
		// if (!dir.endsWith("/")) dir = dir + "/";
		// FileObject diro = FS_MANAGER.resolveFile(dir);
		// if (!diro.exists()) {
		// diro.createFolder();
		// logger.debug("Directory [" + dir + "] created.");
		// }
		// String file = path.substring(pos + 1);
		// FileObject fo = FS_MANAGER.resolveFile(diro, file);
		FileObject fo = FS_MANAGER.resolveFile(path);
		FileObject diro = fo.getParent();
		if (!diro.exists()) {
			logger.debug("Directory [" + diro.toString() + "] not existed, try to create.");
			diro.createFolder();
		}
		if (!fo.exists()) {
			logger.debug("File [" + fo.toString() + "] not existed, try to create.");
			fo.createFile();
			// logger.debug("File [" + file + "] in dir [" + dir + "] created.");
		}
		return fo;
	}

	public static class Driver implements net.butfly.albatis.Connection.Driver<VfsConnection> {
		static {
			DriverManager.register(new Driver());
		}

		@Override
		public VfsConnection connect(URISpec uriSpec) throws IOException {
			return new VfsConnection(uriSpec);
		}

		@Override
		public List<String> schemas() {
			return Colls.list(SCHEMAS);
		}
	}

	private final static String[] SCHEMAS = new String[] { "file", "file:hdfs" // basic
			, "file:ftp", "file:ftps", "file:sftp" // ftp is well supported
			// ,"file:http", "file:https" // readony
			// ,"file:webdav" // full supported (include version)
			// ,"file:temp", "file:ram", "file:res" // utils
			// ,"file:cifs", "file:mime"// sandbox
			// ,"file:jar", "file:tar", "file:zip" //compress with read only
			, "file:gzip", "file:bzip2" // compress with read and write
			// more schema
			, "vfs:hdfs" // basic
			, "vfs:ftp", "vfs:ftps", "vfs:sftp" // ftp is well supported
			// ,"vfs:http", "vfs:https" // readony
			// ,"vfs:webdav" // full supported (include version)
			// ,"vfs:temp", "vfs:ram", "vfs:res" // utils
			// ,"vfs:cifs", "vfs:mime"// sandbox
			// ,"vfs:jar", "vfs:tar", "vfs:zip" //compress with read only
			, "vfs:gzip", "vfs:bzip2" // compress with read and write
			// raw schema
			, "hdfs" // basic
			, "ftp", "ftps", "sftp" // ftp is well supported
			// ,"http", "https" // readony
			// ,"webdav" // full supported (include version)
			// ,"temp", "ram", "res" // utils
			// ,"cifs", "mime"// sandbox
			// ,"jar", "tar", "zip" //compress with read only
			, "gzip", "bzip2" // compress with read and write}
	};

	private static FileSystemManager init() throws FileSystemException {
		// FileSystemOptions opts = new FileSystemOptions();
		// FtpFileSystemConfigBuilder.getInstance().setPassiveMode(opts, true);
		return VFS.getManager();
	}

	public static FileObject vfs(String vfs) throws FileSystemException {
		if (null == FS_MANAGER) FS_MANAGER = init();
		return FS_MANAGER.resolveFile(vfs);
	}

	public static InputStream[] readAll(String vfs) {
		List<InputStream> fs = Colls.list();
		try (FileObject conf = VfsConnection.vfs(vfs);) {
			if (!conf.exists()) throw new IllegalArgumentException("Hbase configuration [" + vfs + "] not existed.");
			if (!conf.isFolder()) throw new IllegalArgumentException("Hbase configuration [" + vfs + "] is not folder.");
			for (FileObject f : conf.getChildren()) if (f.isFile() && "xml".equals(f.getName().getExtension())) {//
				logger.debug("Hbase configuration resource adding: " + f.getName());
				fs.add(new ByteArrayInputStream(IOs.readAll(f.getContent().getInputStream())));
			}
			return fs.toArray(new InputStream[0]);
		} catch (IOException e) {
			throw new IllegalArgumentException("Hbase configuration [" + vfs + "] read fail.", e);
		}
	}

}
