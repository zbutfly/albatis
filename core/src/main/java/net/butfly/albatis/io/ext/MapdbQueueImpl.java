package net.butfly.albatis.io.ext;
//package net.butfly.albatis.io;
//
//import java.io.File;
//import net.butfly.albacore.io.lambda.Function;
//
//import org.mapdb.DB;
//import org.mapdb.DBMaker;
//
//import net.butfly.albatis.io.queue.QueueOddImpl;
//
//public abstract class MapdbQueueImpl<V0> extends OddQueue<V0> {
//	private static final String FILENAME_PREFIX = "mapdb-";
//	private static final String FILENAME_EXT = ".db";
//	protected final DB db;
//	protected final Function<V0, byte[]> conv;
//	protected final Function<byte[], V0> unconv;
//
//	protected MapdbQueueImpl(String name, long capacity, String filename, Function<V0, byte[]> conv, Function<byte[], V0> unconv) {
//		super(name, capacity);
//		this.conv = conv;
//		this.unconv = unconv;
//		if (null == filename) {
//			logger().debug("Temp file MapDB created, for devel and debug only...");
//			db = DBMaker.newTempFileDB().mmapFileEnableIfSupported().closeOnJvmShutdown().make();
//		} else {
//			db = DBMaker.newFileDB(regularFile(filename)).mmapFileEnableIfSupported().closeOnJvmShutdown().make();
//		}
//		closing(() -> db.close());
//		open();
//	}
//
//	private File regularFile(String filename) {
//		File f = new File(filename);
//		String fname = f.getName();
//		if (!fname.startsWith(FILENAME_PREFIX)) fname = FILENAME_PREFIX + "-" + fname;
//		if (!fname.endsWith(FILENAME_EXT)) fname = fname + FILENAME_EXT;
//		return new File(f.getPath() + File.pathSeparator + fname);
//	}
//}