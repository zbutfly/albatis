package net.butfly.albatis.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Mutation;

import net.butfly.albatis.hbase.utils.Hbases;
import net.butfly.albatis.io.Rmap;

@Deprecated
public final class HbaseConvOutput extends HbaseOutput {
	private static final long serialVersionUID = 3301721159039265076L;
	private transient HbaseConvConnection conn;

	public HbaseConvOutput(String name, HbaseConvConnection hconn) throws IOException {
		super(name, hconn);
	}

	@Override
	protected Mutation op(Rmap m) {
		return Hbases.Results.op(m, conn.conv::apply);
	}
}
