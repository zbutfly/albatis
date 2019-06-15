package net.butfly.albatis.bcp.imports.trans;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.bcp.imports.frame.HandleFrame;
import net.butfly.albatis.bcp.imports.frame.conf.ReadConfs;
import net.butfly.albatis.bcp.imports.frame.struct.KernelInfo;
import org.dom4j.DocumentException;

import java.util.List;

/**
 * nothing.
 *
 * @author : kqlu
 * @version :
 * @code : @since : Created in 19:26 2019/2/28
 */
public class TransToZIP {
	public static void ZIP(String path, URISpec uri) throws DocumentException {
		List<KernelInfo> kernelInfos = ReadConfs.getKernelInfos(path);
		for (KernelInfo kernelInfo : kernelInfos) {
			HandleFrame handleFrame = new HandleFrame(kernelInfo, false, uri);
			new Thread(handleFrame).start();
		}
	}
}
