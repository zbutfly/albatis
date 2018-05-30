package com.hzcominfo.dataggr.spark.integrate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.io.Message;

public class MyForeachWriter extends ForeachWriter<Row> {
	private static final long serialVersionUID = -1072922526110204753L;
	private List<Message> ms = Colls.list();

	public List<Message> getMs() {
		return ms;
	}

	@Override
	public void close(Throwable arg0) {
	}

	@Override
	public boolean open(long arg0, long arg1) {
		return true;
	}

	@Override
	public void process(Row row) {
		StructType schema = row.schema();
		String[] fieldNames = schema.fieldNames();
		Map<String, Object> map = new HashMap<>();
		for (String fn : fieldNames) {
			map.put(fn, row.getAs(fn));
		}
		System.out.println(map);
		Message message = new Message(map);
		ms.add(message);
	}
}
