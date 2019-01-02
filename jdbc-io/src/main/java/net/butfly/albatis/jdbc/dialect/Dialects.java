package net.butfly.albatis.jdbc.dialect;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.io.Rmap;

public interface Dialects {
	static void setObject(PreparedStatement ps, int index, Object value) throws SQLException {
		if (value instanceof java.util.Date) {
			java.util.Date jud = (java.util.Date) value;
			// java.sql.Date jsd = new java.sql.Date(jud.getTime()); // 会丢掉时分秒，因java.sql.Date 是不保存time的
			Timestamp jst = new Timestamp(jud.getTime());
			ps.setObject(index, jst);
		} else {
			ps.setObject(index, value);
		}
	}

	static String determineKeyField(List<Rmap> list) {
		if (Colls.empty(list)) return null;
		Rmap msg = list.get(0);
		Object key = msg.key();
		if (null == key) return null;
		return msg.entrySet().stream().filter(e -> key.equals(e.getValue())).map(Map.Entry::getKey).findFirst().orElse(null);
	}
}
