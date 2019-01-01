package net.butfly.albatis.hbase;

import java.io.IOException;
import java.util.Map;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albatis.Connection;
import net.butfly.albatis.ddl.TableDesc;

@Deprecated
public class HbaseConvConnection extends HbaseConnection {
	public final Function<Map<String, Object>, byte[]> conv;

	public HbaseConvConnection(URISpec uri) throws IOException {
		super(uri);
		conv = Connection.uriser(uri);
	}

	@SuppressWarnings("unchecked")
	@Override
	public HbaseOutput outputRaw(TableDesc... table) throws IOException {
		return new HbaseConvOutput("HbaseOutput", this);
	}
}
