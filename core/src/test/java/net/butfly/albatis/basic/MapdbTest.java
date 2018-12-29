package net.butfly.albatis.basic;
//import java.io.File;
//import java.util.concurrent.BlockingQueue;
//
//import org.mapdb.DB;
//import org.mapdb.DBMaker;
//import org.mapdb.Serializer;
//
//public class MapdbTest {
//	public static void main(String... args) {
//		DB db = DBMaker.newFileDB(new File("./mapdb.data")).mmapFileEnableIfSupported().deleteFilesAfterClose().make();
//		try {
//			BlockingQueue<String> q = db.createQueue("queue", Serializer.STRING, false);
//			q.add("!!!");
//		} finally {
//			db.close();
//		}
//	}
//
//	// public static void test3() {
//	// try (DB tdb =
//	// DBMaker.fileDB("./mapdb.data").fileMmapEnable().fileDeleteAfterClose().fileChannelEnable().make())
//	// {
//	// SerializerString ser = new SerializerString();
//	// Map<String, String> map = tdb.treeMap("map", ser, ser).createOrOpen();
//	// List<String> list = tdb.indexTreeList("list", ser).createOrOpen();
//	// System.out.println("created");
//	// map.put("a", "aaa");
//	// map.put("b", "aab");
//	// map.put("c", "aac");
//	// map.put("d", "aad");
//	// list.addAll(map.values());
//	// System.out.println("used");
//	// }
//	// }
//}