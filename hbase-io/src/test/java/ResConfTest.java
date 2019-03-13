import java.io.IOException;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.Connection;
import net.butfly.albatis.hbase.HbaseConnection;

public class ResConfTest {
	// private static final String CONF_URL = "file:///~/Workspaces/dataggr/dpc/subject/src/test/resources/";
	private static final String CONF_URL = "zip:http://data01:7180/cmf/services/35/client-config!/hbase-conf";
	// private static final String CONF_URL = "file:///home/zx/Workspaces/dataggr/dpc/subject/src/test/resources/";

	public static void main(String[] args) throws IOException {
		try (HbaseConnection c = Connection.connect(new URISpec("hbase:" + CONF_URL));) {
			System.err.println("Tables: " + c.ls());
		}
	}
}
