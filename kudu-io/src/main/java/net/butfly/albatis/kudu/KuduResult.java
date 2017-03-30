package net.butfly.albatis.kudu;

import java.util.Map;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Upsert;

import net.butfly.albacore.io.Message;
import net.butfly.albacore.serder.JsonSerder;

/**
 * @Author Naturn
 *
 * @Date 2017年3月16日-下午8:22:18
 *
 * @Version 1.0.0
 *
 * @Email juddersky@gmail.com
 */

public class KuduResult extends Message<String, Upsert, KuduResult> {
	private static final long serialVersionUID = -5843704512434056538L;
	private KuduTable table;
	private final Map<String, Object> result;

	public KuduResult(Map<String, Object> result, KuduTable kuduTable) {
		this.result = result;
		this.table = kuduTable;
	}

	@SuppressWarnings("unchecked")
	public KuduResult(byte[] b) {
		this.result = JsonSerder.JSON_MAPPER.fromBytes(b, Map.class);
	}

	@Override
	public String partition() {
		return table.getName();
	}

	@Override
	public String toString() {
		return table.toString() + "\n\t" + result.toString();
	}

	@Override
	public Upsert forWrite() {
		Upsert upsert = table.newUpsert();
		PartialRow row = upsert.getRow();
		for (ColumnSchema c : table.getSchema().getColumns())
			KuduCommon.generateColumnData(c.getType(), row, c.getName(), result.get(c.getName()));
		return upsert;
	}

}
