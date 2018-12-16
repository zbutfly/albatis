package net.butfly.albatis.kudu;

import static net.butfly.albacore.paral.Sdream.of;
import static net.butfly.albatis.ddl.vals.ValType.Flags.BINARY;
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
import java.util.Arrays;
import java.util.List;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.SessionConfiguration.FlushMode;

import com.google.common.base.Joiner;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.BiConsumer;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;

@SuppressWarnings("unchecked")
public class KuduConnection extends KuduConnectionBase<KuduConnection, KuduClient, KuduSession> {
	public KuduConnection(URISpec kuduUri) throws IOException {
		super(kuduUri);
		session = client.newSession();
		session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND);
		session.setTimeoutMillis(Long.parseLong(Configs.get(KuduProps.TIMEOUT, "2000")));
	}

	@Override
	protected KuduClient initialize(URISpec uri) {
		return new KuduClient.KuduClientBuilder(uri.getHost()).build();
	}

	@Override
	public void commit() {
		List<OperationResponse> v;
		try {
			v = session.flush();
		} catch (KuduException e) {
			logger.error("Kudu commit fail", e);
			return;
		}
		of(v).eachs(r -> {
			if (r.hasRowError()) error(r);
		});
	}

	@Override
	protected KuduTable openTable(String table) {
		try {
			return client.openTable(table);
		} catch (KuduException e) {
			logger().error("Kudu table open fail", e);
			return null;
		}
	}

	private static final Class<? extends KuduException> c;

	static {
		Class<? extends KuduException> cc = null;
		try {
			cc = (Class<? extends KuduException>) Class.forName("org.apache.kudu.client.NonRecoverableException");
		} catch (ClassNotFoundException e) {} finally {
			c = cc;
		}
	}

	public static boolean isNonRecoverable(KuduException e) {
		return null != c && c.isAssignableFrom(e.getClass());
	}

	@Override
	public boolean apply(Operation op, BiConsumer<Operation, Throwable> error) {
		if (null == op) return false;
		// opCount.incrementAndGet();
		boolean r = true;
		OperationResponse or = null;
		try {
			try {
				or = session.apply(op);
			} catch (KuduException e) {
				if (isNonRecoverable(e)) logger.error("Kudu apply fail non-recoverable: " + e.getMessage());
				else error.accept(op, e);
				return (r = false);
			}
			if (null == or) return (r = true);
			if (!(r = !or.hasRowError())) error.accept(op, new IOException(or.getRowError().toString()));
			return r;
		} finally {
			// (r ? succCount : failCount).incrementAndGet();
		}
	}

	@Override
	public void destruct(String name) {
		try {
			if (!client.tableExists(name)) logger.warn("Kudu table [" + name + "] not exised, need not dropped.");
			else {
				logger.warn("Kudu table [" + name + "] exised and dropped.");
				client.deleteTable(name);
			}
		} catch (KuduException ex) {
			logger.warn("Kudu table [" + name + "] drop fail", ex);
		}
	}

	@Override
	protected void construct(String table, ColumnSchema... cols) {
		try {
			if (client.tableExists(table)) {
				logger.warn("Ask for creating new table but existed and not droped, ignore");
				return;
			}
		} catch (KuduException e) {
			throw new RuntimeException(e);
		}
		List<String> keys = new ArrayList<>();
		for (ColumnSchema c : cols)
			if (c.isKey()) keys.add(c.getName());
			else break;

		int buckets = Integer.parseInt(System.getProperty(KuduProps.TABLE_BUCKETS, "24"));
		String v = Configs.get(KuduProps.TABLE_REPLICAS);
		int replicas = null == v ? -1 : Integer.parseInt(v);
		String info = "with bucket [" + buckets + "], can be defined by [-D" + KuduProps.TABLE_BUCKETS + "=8(default value)]";
		if (replicas > 0) info = info + ", with replicas [" + replicas + "], can be defined by [-D" + KuduProps.TABLE_REPLICAS
				+ "=xx(no default value)]";
		logger.info("Kudu table [" + table + "] will be created with keys: [" + Joiner.on(',').join(keys) + "], " + info);
		CreateTableOptions opts = new CreateTableOptions().addHashPartitions(keys, buckets);
		if (replicas > 0) opts = opts.setNumReplicas(replicas);
		try {
			client.createTable(table, new Schema(Arrays.asList(cols)), opts);
		} catch (KuduException e) {
			throw new RuntimeException(e);
		}
		logger.info("Kudu table [" + table + "] created successfully.");
	}

	@Override
	public void construct(String dbName, String table, TableDesc tableDesc, List<FieldDesc> fields) {
		List<ColumnSchema> columns = Colls.list();
		List<ColumnSchema> columns2 = Colls.list();
		List<String> keys = new ArrayList<>();
		CreateTableOptions tableOptions = new CreateTableOptions();
		for (FieldDesc field : fields) {
			// 创建列
			if (tableDesc.keys.get(0).contains(field.name)) {
				Type type = buildKuduFieldType(field);
				ColumnSchema.ColumnSchemaBuilder keyBuilder = new ColumnSchema.ColumnSchemaBuilder(field.name, type).encoding(type.equals(
						Type.STRING) ? ColumnSchema.Encoding.DICT_ENCODING : ColumnSchema.Encoding.BIT_SHUFFLE).compressionAlgorithm(
								ColumnSchema.CompressionAlgorithm.LZ4).nullable(false).key(true);
				columns2.add(keyBuilder.build());
				keys.add(field.name);
			} else {
				Type type = buildKuduFieldType(field);
				ColumnSchema.ColumnSchemaBuilder builder = new ColumnSchema.ColumnSchemaBuilder(field.name, type).encoding(type.equals(
						Type.STRING) ? ColumnSchema.Encoding.DICT_ENCODING : ColumnSchema.Encoding.BIT_SHUFFLE).compressionAlgorithm(
								ColumnSchema.CompressionAlgorithm.LZ4).nullable(true).key(false);
				columns.add(builder.build());
			}
		}
		columns.forEach(column -> columns2.add(column));

		// 创建schema
		Schema schema = new Schema(columns2);
		if (null == tableDesc.construct || tableDesc.construct.size() == 0) {
			tableOptions.addHashPartitions(keys, 3);
			try {
				client.createTable(table, schema, tableOptions);
			} catch (KuduException e1) {
				e1.printStackTrace();
			}
		} else {
			// 设置hash分区和数量
			Object bucket = tableDesc.construct.get("bucket");
			if (null == bucket)
				// 用列做为分区的参照
				tableOptions.setRangePartitionColumns(keys);
			else
			// 添加key的hash分区
			tableOptions.addHashPartitions(keys, Integer.parseInt(bucket.toString()));
			// 创建table,并设置partition
			try {
				client.createTable(table, schema, tableOptions);
			} catch (KuduException e) {
				e.printStackTrace();
			}
		}
	}

	private Type buildKuduFieldType(FieldDesc field) {
		switch (field.type.flag) {
		case SHORT:
			return Type.INT8;
		case INT:
			return Type.INT16;
		case LONG:
			return Type.INT64;
		case BINARY:
			return Type.BINARY;
		case STR:
		case CHAR:
		case UNKNOWN:
			return Type.STRING;
		case BYTE:
		case BOOL:
			return Type.BOOL;
		case FLOAT:
			return Type.FLOAT;
		case DOUBLE:
			return Type.DOUBLE;
		case DATE:
			return Type.UNIXTIME_MICROS;
		default:
			return Type.STRING;
		}
	}

	@Override
	public boolean judge(String dbName, String table) {
		boolean exists = false;
		try {
			exists = client.tableExists(table);
		} catch (KuduException e) {
			e.printStackTrace();
		}
		return exists;
	}
}
