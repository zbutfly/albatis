package net.butfly.albatis.io.utils;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public interface JsonUtils {
	static final ObjectMapper json = new ObjectMapper();

	@Deprecated
	static List<String> parseFieldsByJson(Object fields) { // TODO: no native json
		try {
			return json.readValue(stringify(fields), json.getTypeFactory().constructCollectionType(List.class, String.class));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	static String stringify(Object object) {
		try {
			return json.writeValueAsString(object);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}
}
