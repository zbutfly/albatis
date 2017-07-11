package net.butfly.albatis.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Connection;

import com.hzcominfo.albatis.nosql.NoSqlConnection;

import net.butfly.albacore.io.utils.URISpec;
import net.butfly.albacore.utils.logger.Logger;

public class HbaseConnection extends NoSqlConnection<Connection> {
	private static final Logger logger = Logger.getLogger(HbaseConnection.class);

	static {
		com.hzcominfo.albatis.nosql.Connection.register("hbase", HbaseConnection.class);
	}

	public HbaseConnection(URISpec uriSpec) throws IOException {
		super(uriSpec, u -> {
			try {
				return Hbases.connect();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		} , "hbase");
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
