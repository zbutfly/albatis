import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;

import net.butfly.albacore.utils.collection.Maps;

public class ArangodbTest {
	public static void main(String[] args) {
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
	}
}
