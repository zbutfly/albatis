package net.butfly.albatis.kudu;

import java.io.IOException;
import java.util.Map;

import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.AsyncKuduSession;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;

import com.hzcominfo.albatis.nosql.NoSqlConnection;
import com.stumbleupon.async.Deferred;

import net.butfly.albacore.io.utils.URISpec;
import net.butfly.albacore.utils.logger.Logger;



public class AsyncKuduConnection extends NoSqlConnection<AsyncKuduClient> {

	private static final Logger logger = Logger.getLogger(AsyncKuduConnection.class);

	protected AsyncKuduConnection(URISpec kuduUri, Map<String, String> props) throws IOException {
		super(kuduUri, r -> {
				return new AsyncKuduClient.AsyncKuduClientBuilder(kuduUri.getHost()).build();			
		}, "Async", "kudu");
	}

	public AsyncKuduConnection(String kuduUri, Map<String, String> props) throws IOException {
		this(new URISpec(kuduUri), props);
	}
	
	@Override
	public void close() {
		try {
			super.close();
			client().close();
		} catch (Exception e) {
			logger.error("Close failure", e);
		}
	}

	public Deferred<KuduTable> kuduTable(String table) throws KuduException {
		return super.client().openTable(table);
	}

	public AsyncKuduSession newSession() {
		return super.client().newSession();
	}
}
