package net.butfly.albatis.kudu;

import java.util.Map;

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

public class KuduResult extends Message<String, Map<String, Object>, KuduResult> {
	private static final long serialVersionUID = -5843704512434056538L;
	private String table;
	private final Map<String, Object> result;

	public KuduResult(Map<String, Object> result, String table) {
		this.table = table;
		this.result = result;
	}

	@SuppressWarnings("unchecked")
	public KuduResult(byte[] source) {
		this.result = JsonSerder.JSON_MAPPER.fromBytes(source, Map.class);
		this.table = "";
	}

	@Override
	public String partition() {
		return table;
	}

	@Override
	public String toString() {
		return table.toString() + "\n\t" + result.toString();
	}

	@Override
	public Map<String, Object> forWrite() {
		return result;
	}

}
