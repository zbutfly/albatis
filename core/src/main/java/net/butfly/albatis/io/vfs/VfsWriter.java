package net.butfly.albatis.io.vfs;

import static net.butfly.albacore.utils.IOs.flushAndClose;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.provider.AbstractFileObject;

import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albatis.io.Rmap;

public class VfsWriter implements Closeable {
	private final VfsConnection vfs;
	private final String table;
	private final AtomicLong count = new AtomicLong();

	private FileObject fo;
	private FileContent fc;
	private OutputStream os;
	private BufferedOutputStream bos;
	private PrintWriter fw;

	public VfsWriter(VfsConnection vfs, String table) {
		this.vfs = vfs;
		this.table = table;
		open();
	}

	public long write(Rmap m) {
		if (vfs.limit <= 0) {
			fw.println(JsonSerder.JSON_MAPPER.ser(m));
			return count.incrementAndGet();
		} else return count.updateAndGet((origin) -> {
			if (origin >= vfs.limit) /* synchronized (this) */ {// exceed
				close();
				open();
				origin = 0;
			}
			fw.println(JsonSerder.JSON_MAPPER.ser(m));
			return origin + 1;
		});
	}

	@SuppressWarnings("rawtypes")
	private void open() {
		try {
			fo = refresh(fo, table);
			if (fo instanceof AbstractFileObject) {
				fc = null;
				os = os((AbstractFileObject) fo);
			} else {
				fc = fo.getContent();
				os = os(fc);
			}
			bos = new BufferedOutputStream(os);
		} catch (FileSystemException e) {
			throw new RuntimeException(e);
		}
		fw = new PrintWriter(bos);
	}

	@SuppressWarnings("rawtypes")
	private static OutputStream os(AbstractFileObject f) throws FileSystemException {
		try {
			return f.getOutputStream(true);
		} catch (FileSystemException e) {
			VfsConnection.logger.warn("File open for APPEND failed, try normal mode.", e);
			return f.getOutputStream();
		}
	}

	private static OutputStream os(FileContent f) throws FileSystemException {
		try {
			return f.getOutputStream(true);
		} catch (FileSystemException e) {
			VfsConnection.logger.warn("File open for APPEND failed, try normal mode.", e);
			return f.getOutputStream();
		}
	}

	private synchronized FileObject refresh(FileObject curr, String fn) throws FileSystemException {
		if (null != curr) curr.close();
		int pos = fn.lastIndexOf('.');
		String ext;
		if (pos < 0) ext = vfs.ext;
		else {
			ext = fn.substring(pos + 1);
			fn = fn.substring(0, pos);
		}
		Date now = new Date();
		if (null != vfs.prefix) fn = new SimpleDateFormat(vfs.prefix).format(now) + fn;
		if (null != vfs.suffix) fn = fn + new SimpleDateFormat(vfs.suffix).format(now);
		fn = fn + "." + ext;
		return curr = vfs.open(fn);

	}

	@Override
	public synchronized void close() {
		flushAndClose(fw);
		flushAndClose(bos);
		flushAndClose(os);
		flushAndClose(fc);
		flushAndClose(fo);
	}
}
