package net.butfly.albatis.cassandra;


import static net.butfly.albatis.ddl.vals.ValType.Flags.BOOL;
import static net.butfly.albatis.ddl.vals.ValType.Flags.BYTE;
import static net.butfly.albatis.ddl.vals.ValType.Flags.CHAR;
import static net.butfly.albatis.ddl.vals.ValType.Flags.DATE;
import static net.butfly.albatis.ddl.vals.ValType.Flags.DOUBLE;
import static net.butfly.albatis.ddl.vals.ValType.Flags.FLOAT;
import static net.butfly.albatis.ddl.vals.ValType.Flags.INT;
import static net.butfly.albatis.ddl.vals.ValType.Flags.LONG;
import static net.butfly.albatis.ddl.vals.ValType.Flags.SHORT;
import static net.butfly.albatis.ddl.vals.ValType.Flags.STR;
import static net.butfly.albatis.ddl.vals.ValType.Flags.UNKNOWN;

import java.io.IOException;

import java.util.ArrayList;

import java.util.List;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;

import net.butfly.albacore.io.URISpec;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.TableDesc;


public class CassandraConnection extends DataConnection<Cluster> {
	
	protected final static Logger logger = Logger.getLogger(CassandraConnection.class);
	
	protected final String defaultKeyspace;
	
	public  CassandraConnection(URISpec uri) throws IOException {
		super(uri,"cassandra");	
		if(null != uri.getFile()) {
			defaultKeyspace = uri.getFile();
		}else {
			defaultKeyspace = null;
		}		
		
	}
	
	
	@Override
	protected Cluster initialize(URISpec uri) {
		// TODO Auto-generated method stub
		Builder builder = Cluster.builder();
		Boolean flag = false;
		try {
			if (uri.getPaths().length == 0) {
				String[] host = uri.getHost().split(":");
				builder.addContactPoint(host[0]);
				return builder.withPort(Integer.parseInt(host[1])).build();
			} else {
				flag = true;
				if (flag) {
					builder.addContactPoint(uri.getHost());
				}
				for (int i = 0; i < uri.getPaths().length - 1; i++) {
					builder.addContactPoint(uri.getPaths()[i]);
				}
				String[] lastAddress = uri.getPaths()[uri.getPaths().length - 1].split(":");
				builder.addContactPoint(lastAddress[0]);
				return builder.withPort(Integer.parseInt(lastAddress[1])).build();
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
	}
 	
	public static class Driver implements net.butfly.albatis.Connection.Driver<CassandraConnection> {
		static {
			DriverManager.register(new Driver());
		}

		@Override
		public CassandraConnection connect(URISpec uriSpec) throws IOException {
			return new CassandraConnection(uriSpec);
		}
		@Override
		public List<String> schemas() {
			return Colls.list("cassandra");
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public CassandraInput inputRaw(TableDesc... table) throws IOException {
		
		CassandraInput i = null;
		try {
			i = new CassandraInput("CassandraInput", this,table[0].qualifier.name);
			System.out.println(table[0].qualifier.name);
			
		} catch (Exception e) {
			new IOException(e);
		}
		return i;
	}

	
	@SuppressWarnings("unchecked")
	@Override
	public CassandraOutput outputRaw(TableDesc... table) throws IOException{
		
		return new CassandraOutput("CassandraOutnput", defaultKeyspace, table[0].qualifier.name,this);
		
	}
	
	
	@Override
	public void close() {
		try {
			super.close();
		} catch (IOException e) {
			logger.error("Close failure", e);
		}
		client.close();
	}
	
	
	public String buildCreateTableCql(String table, FieldDesc... fields) {
		StringBuilder sql = new StringBuilder();
		List<String> fieldCql = new ArrayList<>();
		for (FieldDesc f : fields)
			fieldCql.add(buildField(f));
		sql.append("CREATE TABLE if not exists ").append(defaultKeyspace).append(".")
		.append(table).append(" (").append(String.join(",", fieldCql.toArray(new String[0]))).append(")");
		return sql.toString();
	}

	private static String buildField(FieldDesc field) {
		StringBuilder sb = new StringBuilder();
		switch (field.type.flag) {
			case BOOL:
			case BYTE:
				sb.append(field.name).append(" tinyint");
				break;
			case SHORT:
				sb.append(field.name).append(" smallint");
				break;
			case INT:
				sb.append(field.name).append(" int");
				break;
			case LONG:
				sb.append(field.name).append(" bigint");
				break;
			case FLOAT:
				sb.append(field.name).append(" float");
				break;
			case DOUBLE:
				sb.append(field.name).append(" double");
				break;
			case STR:
			case CHAR:
			case UNKNOWN:
				sb.append(field.name).append(" text");
				break;
			case DATE:
				sb.append(field.name).append(" timestamp");
				break;
			default:
				break;
		}
		if (field.rowkey) sb.append(" primary key");
		return sb.toString();
	}
	@Override
	public void construct(Qualifier table, FieldDesc... fields) {
		try {
			String cql = buildCreateTableCql(table.name, fields);
			if(null!= client) {
				client.connect().execute(cql);
			}
			 
		} catch (Exception e) {
			logger().error("construct table failure", e);
		}
	}

	

}
