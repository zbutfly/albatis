package net.butfly.albatis.kudu.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduScanner;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.kudu.KuduConnection;

public class KuduTest {

	public static void main(String[] args) throws IOException {
		testScanner();
		// testPump();
	}

	public static void testScanner() throws IOException {
		String url = "kudu://172.30.10.31:7051,172.30.10.32:7051,172.30.10.33:7051/KUDU";
		try (KuduConnection kuduConnection = new KuduConnection(new URISpec(url))) {
			KuduClient client = kuduConnection.client;
			List<ColumnSchema> columnSchemas = client.openTable("TTTTTTT222").getSchema().getColumns();
			List<String> columnNameList = new ArrayList<>();
			columnSchemas.forEach(columnSchema -> columnNameList.add(columnSchema.getName().toLowerCase()));
			KuduScanner scanner = client.newScannerBuilder(client.openTable("TTTTTTT222")).setProjectedColumnNames(columnNameList).build();
			System.out.println(scanner.nextRows().next());
		}
	}

	// public static void testPump() throws IOException{
	// String url = "kudu://172.30.10.31:7051,172.30.10.32:7051,172.30.10.33:7051/KUDU";
	// String sourceTable = "TTTTTTT222";
	// String mysqlUrl =
	// "jdbc:mysql://172.16.17.22:3306/xh_policealarm_dev?user=xh_policealarm_dev&password=Xh_policealarm@dev123!&useSSL=false";
	// String targetTable = "TEST_PUMP";
	// Input<Rmap> input = Connection.connect(new URISpec(url)).input(sourceTable);
	// try (Output<Rmap> output = Connection.connect(new URISpec(mysqlUrl)).output(targetTable);
	// Pump<Rmap> p = input.then(m ->m.table(targetTable)).pump(10, output)) {
	// p.open();
	// }
	// }

}
