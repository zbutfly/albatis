//package net.butfly.albatis.kafka.deprecated;
//
//import java.io.File;
//import java.io.IOException;
//
//import net.butfly.albacore.exception.ConfigException;
//import net.butfly.albacore.utils.Systems;
//
//@Deprecated
//public class Local {
//	static String curr(String base, String folder) throws ConfigException {
//		try {
//			String path = base + "/" + Systems.getMainClass().getSimpleName() + "/" + folder.replaceAll("[:/\\,]", "-");
//			File f = new File(path);
//			f.mkdirs();
//			return f.getCanonicalPath();
//		} catch (IOException e) {
//			throw new ConfigException(e);
//		}
//	}
//}
