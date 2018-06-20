import java.io.IOException;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.arangodb.ArangoConnection;
import net.butfly.albatis.arangodb.ArangoInput;

public class ArangodbTest {
/*	public static void main(String[] args) {
		ArangoDB adb = new ArangoDB.Builder().host("192.168.182.50", 8888).build();
		try {
			adb.createDatabase("sample");
			ArangoDatabase db = adb.db("sample");
			db.createCollection("table");
			ArangoCollection col = db.collection("help");
			col.insertDocument(Maps.of("name", "zhang"));
		} finally {
			adb.shutdown();
		}
	}*/
	/*public static void main(String[] args) {
		URISpec uri = new URISpec("");
		try {
			ArangoConnection conn = new ArangoConnection(uri);
			ArangoInput in = new ArangoInput("arango_test", conn);
			m
		} catch (IOException e) {
			Logger.getLogger(ArangodbTest.class).info("");
		}
	}*/
}
