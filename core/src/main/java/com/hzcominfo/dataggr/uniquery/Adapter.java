package com.hzcominfo.dataggr.uniquery;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.gson.JsonObject;
import com.hzcominfo.albatis.nosql.Connection;
import com.hzcominfo.dataggr.uniquery.utils.ExceptionUtil;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Reflections;

public abstract class Adapter {
	public Adapter() {}
	
	static final Map<String, Class<? extends Adapter>> ADAPTER_MAP = loadAdapters();
	
	// 查询组装
    public abstract <T> T queryAssemble(Connection connection, JsonObject sqlJson);
    
    // 查询执行
    public abstract <T> T queryExecute(Connection connection, Object query, String table);
    
    // 结果组装
    public abstract <T> T resultAssemble(Object result);
    
    // 加载子类
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

	// 根据uriSpec获取对应的适配器实现
	@SuppressWarnings("unchecked")
	public static <T extends Adapter> T adapt(URISpec uriSpec) {
		for (String schema : ADAPTER_MAP.keySet()) {
			if (schema.contains(uriSpec.getScheme())) {
				try {
					return (T) adapt(ADAPTER_MAP.get(schema));
				} catch (Exception e) {
					ExceptionUtil.runtime("Constructor error: ", e);
				}
			}
		}
		throw new RuntimeException("No matched connection");
	}
    
	static <T extends Adapter> T adapt(Class<T> clazz)
			throws Exception {
		Constructor<T> con = clazz.getConstructor();
		return con.newInstance();
	}
}
