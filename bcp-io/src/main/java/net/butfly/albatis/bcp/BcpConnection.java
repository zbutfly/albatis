package net.butfly.albatis.bcp;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.Connection;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 * @author zhuqh
 */
public class BcpConnection extends DataConnection<Connection> {
	final static String schema = "bcp";
	private static final Logger logger = Logger.getLogger(BcpConnection.class);
	final String dataPath;

	public BcpConnection(URISpec urispec) throws IOException {
		super(urispec, schema);
		dataPath = urispec.getPath();
	}

	@Deprecated
	public BcpConnection(String urispec) throws IOException {
		this(new URISpec(urispec));
	}

	@Override
	public BcpInput inputRaw(TableDesc... table) throws IOException {
		if (table.length > 1) throw new UnsupportedOperationException("Multiple table input");
		BcpInput bcpInput = new BcpInput("BcpInput",dataPath,table[0].qualifier.name);
		return bcpInput;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Output<Rmap> outputRaw(TableDesc... table) throws IOException {
		BcpOutput bcpOutput = new BcpOutput("BcpOutnput",table[0].qualifier.name);
		return bcpOutput;
	}

	@Override
	public void close() {
		try {
			super.close();
		} catch (IOException e) {
			logger.error("Close failure", e);
		}
	}

	public static class Driver implements Connection.Driver<BcpConnection> {
		static {
			DriverManager.register(new Driver());
		}

		@Override
		public BcpConnection connect(URISpec uriSpec) throws IOException {
			return new BcpConnection(uriSpec);
		}

		@Override
		public List<String> schemas() {
			return Colls.list("bcp");
		}
	}
}
