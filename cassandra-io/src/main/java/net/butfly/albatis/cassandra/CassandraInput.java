package net.butfly.albatis.cassandra;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;

import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.Rmap;

public class CassandraInput extends net.butfly.albacore.base.Namedly implements OddInput<Rmap>{
	
	private static final long serialVersionUID = -4789851501849429707L;
	private final CassandraConnection Caconn;
	private String tableName;
	private ResultSet rs;
	private Iterator<Row> it;
	private Session session;
	private SimpleStatement  simplestament;
	public CassandraInput(String name, CassandraConnection conn) {
		super(name);
		Caconn = conn;
		closing(this::closeCassandra);
		
	}
	public CassandraInput(final String name, CassandraConnection conn, String tableName) throws Exception {
		super(name);
		this.tableName = tableName;
		this.Caconn = conn;
		openCassandra();
		closing(this::closeCassandra);
	}
	
	public void query() throws SQLException {
		String cql = "select * from "+ Caconn.defaultKeyspace+"."+tableName;
		session = Caconn.client.connect();
		simplestament = new SimpleStatement(cql);
	
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
		for (Definition key : row.getColumnDefinitions().asList()) {
			msg.put(key.getName(), row.getObject(key.getName()));
		}
		return msg;
		// List<Row> row = rs.all();
	}
	
	private void openCassandra() {
		
		try {
			query();
			rs = session.execute(simplestament);
			it = rs.iterator();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			return;
		}
			
	}
	
	private void closeCassandra() {
		try {
			Caconn.close();
		} catch (Exception e) {
			
		}
	}


}
