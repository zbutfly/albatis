package net.butfly.albatis.kudu;

import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Upsert;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import net.butfly.albacore.io.Message;

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

	private KuduTable kuduTable;

	private static final long serialVersionUID = -5843704512434056538L;

	private JsonObject result;

	private Gson gson = new Gson();

	public KuduResult(JsonObject jsonObject, KuduTable kuduTable) {
		
		this(jsonObject.toString().getBytes());
		this.kuduTable = kuduTable;

	}

	public KuduResult(byte[] b) {
		// TODO Auto-generated constructor stub
		this.result = gson.fromJson(new String(b), JsonObject.class);
	}

	@Override
	public String partition() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Upsert forWrite() {		
		Upsert upsert = kuduTable.newUpsert();
		PartialRow row = upsert.getRow();
		kuduTable.getSchema().getColumns().stream().forEach(p->{
			KuduCommon.generateColumnData(p.getType(), row, p.getName(), result.get(p.getName()));
		});		
		return upsert;
	}

}
