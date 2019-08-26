package net.butfly.albatis.cassandra;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;


import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;

import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;



public class CassandraOutput extends OutputBase<Rmap>{
	
	private final CassandraConnection Caconn;
	private final String keyspace;
	private final String table_name;
	private Session session;
	


	public CassandraOutput(String name, String keyspace, String table_name, CassandraConnection Caconn) {
		super(name);
		this.Caconn = Caconn;
		this.keyspace = keyspace;
		this.table_name = table_name;
		this.session = Caconn.client.connect();
		closing(this.Caconn::close);
	}
	
	
	public void createTable(String space_name,String table_name, Session session) {
		StringBuilder cql = new StringBuilder();
		cql.append("CREATE TABLE if not exists ").append(space_name).append(".").append(table_name)
		.append("(");
		
		
	}

	@Override
	protected void enqsafe(Sdream<Rmap> items) {
		
		List<Rmap> msgs = items.collect();
		//createTable(keyspace,table_name,session);
		if(msgs.size() > 0) {
			try  {
				msgs.forEach(m -> upsert(keyspace, table_name, session, m));
				
			} catch (Exception e) {
				logger().error("failed to insert or update items.");
			}
		}else {
		   return;
		}
		
	}
	
	public void upsert(String spacename, String table_name, Session session, Rmap rmap) {
		List<String> keyList = new ArrayList<>();
		List<Object> valueList = new ArrayList<>();
	
		rmap.map().forEach((k, v) -> {keyList.add(k);valueList.add(v);});

		Insert insert = QueryBuilder.insertInto(spacename, table_name).values(keyList, valueList);
		System.out.println(insert); 
		session.execute(insert);
	}

	@Override
	public URISpec target() {
		return Caconn.uri();
	}
	
}
