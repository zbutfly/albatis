package net.butfly.albatis.kudu.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.*;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.kudu.KuduConnection;

public class KuduTest {

	public static void main(String[] args) throws IOException {
//		testScanner();
		testKuduClient();
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

	public static void testKuduClient() throws IOException {
		String url = "kudu://172.30.10.31:7051,172.30.10.32:7051,172.30.10.33:7051/KUDU";
		String srcTable = "M2KUDU_TEST";
		try (KuduConnection conn = new KuduConnection(new URISpec(url))) {
			KuduTable kuduTable = conn.client.openTable(srcTable);
			Operation op = kuduTable.newUpsert();
			if(op.getTable().getAsyncClient() == kuduTable.getAsyncClient())
				System.out.println("******************************");
			else
				System.out.println("=================================");
		}


	}

}
