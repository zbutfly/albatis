package net.butfly.albatis.cassandra;

import java.util.Iterator;

import com.datastax.oss.driver.api.core.cql.*;


import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Rmap;

public class CassandraInput extends net.butfly.albacore.base.Namedly implements OddInput<Rmap>{
	
	private static final long serialVersionUID = -4789851501849429707L;
	private final CassandraConnection Caconn;
	private String tableName;
	private ResultSet rs;
	private Iterator<Row> it;

  public CassandraInput(String name, CassandraConnection conn) {
		super(name);
		Caconn = conn;
		closing(this::closeCassandra);
		
	}
	public CassandraInput(final String name, CassandraConnection conn, String tableName) throws Exception{
		super(name);
		this.tableName = tableName;
		this.Caconn = conn;
		
		openCassandra();
		closing(this::closeCassandra);
	}
	
	@Override
	public Rmap dequeue() {
		Row row = null;
		Rmap msg;
		synchronized (it) {
			if (!it.hasNext() || null == (row = it.next())) 
				return null;
		}
		msg = new Rmap(new Qualifier(tableName));
		
		for ( ColumnDefinition key : row.getColumnDefinitions()) {
			msg.put(key.getName().toString(), row.getObject(key.getName()));
		}
		return msg;
		// List<Row> row = rs.all();
	}
	
	private void openCassandra() {
		try {
			String cql = "select * from "+ Caconn.defaultKeyspace+"."+tableName;
			rs = Caconn.client.execute(cql);
			it = rs.iterator();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			
		}
		rs = null;	
	}
	
	private void closeCassandra() {
		try {
			Caconn.close();
		} catch (Exception e) {
			
		}
	}
}
