package net.butfly.albatis.kudu;

import java.io.IOException;
import java.util.Map;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;

import com.hzcominfo.albatis.nosql.NoSqlConnection;

import net.butfly.albacore.io.utils.URISpec;
import net.butfly.albacore.utils.logger.Logger;

/**
 * @Author Naturn
 *
 * @Date 2017年3月15日-下午4:17:49
 *
 * @Version 1.0.0
 *
 * @Email juddersky@gmail.com
 */

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
		// TODO Auto-generated constructor stub
	}

	public KuduConnection(String kuduUri, Map<String, String> props) throws IOException {
		// TODO Auto-generated constructor stub
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

}
