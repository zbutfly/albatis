package net.butfly.albatis.cassandra;

import java.io.IOException;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.Connection;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.pump.Pump;

public class Test {
	
	public void execute() {
		
		try (Connection outconn = Connection.connect(new URISpec("kafka://10.33.41.52:2181/kafka?df=str,json"));
				Connection inconn = Connection.connect(new URISpec("cassandra://10.19.120.97:9042/test_cassandra"));
				Input<Rmap> in = inconn.input("Student");
				Output<Rmap> out = outconn.output("test_out_710");
				Pump<Rmap> p = in.then(r -> {
					r.table(new Qualifier("test_out_710"));
					return r;
				}).pump(1, out);) {

			p.open();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Test test = new Test();
		test.execute();

	}

}
