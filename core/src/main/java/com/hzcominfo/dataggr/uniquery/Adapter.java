package com.hzcominfo.dataggr.uniquery;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.sql.SqlNode;

import com.hzcominfo.albatis.nosql.Connection;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Reflections;

public interface Adapter {
	static final Map<String, Class<? extends Adapter>> ADAPTER_MAP = loadAdapters();
	
	static Map<String, Class<? extends Adapter>> loadAdapters() {
		Map<String, Class<? extends Adapter>> map = new HashMap<>();
		Set<Class<? extends Adapter>> set = Reflections.getSubClasses(Adapter.class);
		for (Class<? extends Adapter> c : set) {
			try {
				Field[] fields = c.getDeclaredFields();
				if (fields != null && fields.length > 0) {
					for (Field f : fields) {
						if ("schema".equals(f.getName())) {
							f.setAccessible(true);
							map.put((String) f.get(c), c);
							break;
						}
					}
				}
			} catch (SecurityException | IllegalArgumentException | IllegalAccessException e) {
				throw new RuntimeException("get field error", e);
			}
		}
		return map;
	};

	public static <T extends Adapter> T adapt(URISpec uriSpec) {
//		for (String schema : ADAPTER_MAP.keySet()) {
//			if (schema.contains(uriSpec.getScheme())) {
//				return (T) adapt(uriSpec, ADAPTER_MAP.get(schema));
//			}
//		}
//		throw new RuntimeException("No matched connection");
		return null;
	}
	
	// 查询组装
    public <T> T queryAssemble(SqlNode sqlNode, Object...params);
    
    // 查询执行
    public <T> T queryExecute(Connection connection, Object query);
    
    // 结果组装
    public <T> T resultAssemble(Object result);
}
