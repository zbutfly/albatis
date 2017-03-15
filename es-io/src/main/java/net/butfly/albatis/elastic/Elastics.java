package net.butfly.albatis.elastic;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import net.butfly.albacore.utils.Utils;
import net.butfly.albacore.utils.logger.Logger;

/**
 * @Author Naturn
 *
 * @Date 2017年3月13日-上午10:25:14
 *
 * @Version 1.0.0
 *
 * @Email juddersky@gmail.com
 */

public final class Elastics extends Utils {
	protected static final Logger logger = Logger.getLogger(Elastics.class);

	public static void writeScript(ObjectOutputStream oos, Script script) throws IOException {
		oos.writeUTF(script.getType().name());
		oos.writeUTF(script.getLang());
		oos.writeObject(script.getParams());
	}

	@SuppressWarnings("unchecked")
	public static Script readScript(ObjectInputStream oos) throws IOException {
		// String script = oos.readUTF();
		ScriptType st = ScriptType.valueOf(oos.readUTF());
		String lang = oos.readUTF();
		try {
			return new Script(st, lang, ScriptType.INLINE.toString(), (Map<String, Object>) oos.readObject());
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
	}

	public static void mapping(ElasticConnection connect, Map<String, ?> mapping, String... types) {
		logger.debug("Mapping constructing: " + mapping);
		PutMappingRequest req = new PutMappingRequest(connect.getDefaultIndex());
		req.source(mapping);
		Set<String> tps = new HashSet<>(Arrays.asList(types));
		tps.add(connect.getDefaultType());
		for (String t : tps) {
			req.type(t);
			PutMappingResponse resp = connect.client().admin().indices().putMapping(req).actionGet();
			if (!resp.isAcknowledged())
				logger.error("Mapping failed" + req.toString());
		}
	}
}
