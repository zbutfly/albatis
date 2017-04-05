package net.butfly.albatis.kudu;

import java.io.IOException;
import java.util.Map;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;

import com.hzcominfo.albatis.nosql.NoSqlConnection;

import net.butfly.albacore.io.utils.URISpec;
import net.butfly.albacore.utils.logger.Logger;

public class KuduConnection extends NoSqlConnection<KuduClient> {
	private static final Logger logger = Logger.getLogger(KuduConnection.class);

	protected KuduConnection(URISpec kuduUri, Map<String, String> props) throws IOException {
		super(kuduUri, r -> {
			try (KuduClient client = new KuduClient.KuduClientBuilder(kuduUri.getHost()).build()) {
				return client;
			} catch (KuduException ex) {
				logger.error("Kudu client ini faile.");
			}
			return null;
		}, "kudu", "kudu");
	}

	public KuduConnection(String kuduUri, Map<String, String> props) throws IOException {
		this(new URISpec(kuduUri), props);
	}

	@Override
	public void close() {
		try {
			super.close();
			client().close();
		} catch (IOException e) {
			logger.error("Close failure", e);
		}
	}
	
	public KuduTable kuduTable(String table) throws KuduException{
		
		return super.client().openTable(table);
	}
	
	public KuduSession newSession(){
		
		return super.client().newSession();
	}
}
