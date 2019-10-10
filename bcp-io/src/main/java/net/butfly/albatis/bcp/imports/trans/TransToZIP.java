package net.butfly.albatis.bcp.imports.trans;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Configs;
import net.butfly.albatis.bcp.TaskDesc;
import net.butfly.albatis.bcp.imports.frame.HandleFrame;
import net.butfly.albatis.bcp.imports.frame.conf.ReadConfs;
import net.butfly.albatis.bcp.imports.frame.struct.KernelInfo;
import org.dom4j.DocumentException;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * nothing.
 *
 * @author : kqlu
 * @version :
 * @code : @since : Created in 19:26 2019/2/28
 */
public class TransToZIP {
	public static final ThreadPoolExecutor CACHED_THREAD_POOL = (ThreadPoolExecutor) Executors.newFixedThreadPool(5);
	
    public static void ZIP(String path, URISpec uri, String table) throws DocumentException {
        List<KernelInfo> kernelInfoList = ReadConfs.getKernelInfos(path);
        for (KernelInfo kernelInfo : kernelInfoList) {
            HandleFrame handleFrame = new HandleFrame(kernelInfo, false, uri, table);
            CACHED_THREAD_POOL.submit(handleFrame);
//            new Thread(handleFrame).start();
        }
    }
}
