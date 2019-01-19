package net.butfly.albatis.io.utils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;

import net.butfly.albacore.serder.json.Jsons;

public interface JsonUtils {
	@Deprecated
	static List<String> parseFieldsByJson(Object fields) { // TODO: no native json
		try {
			return Jsons.mapper.readValue(stringify(fields), Jsons.mapper.getTypeFactory().constructCollectionType(List.class,
					String.class));
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	static Map<String, Object> parseObject2Map(Object object) {
		try {
			return Jsons.mapper.readValue(stringify(object), Jsons.mapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	static String stringify(Object object) {
		try {
			return Jsons.mapper.writeValueAsString(object);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}
}
