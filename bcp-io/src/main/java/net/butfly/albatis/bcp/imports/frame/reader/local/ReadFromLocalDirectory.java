package net.butfly.albatis.bcp.imports.frame.reader.local;

import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.bcp.imports.frame.reader.ReadFromDirectory;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

/**
 * nothing.
 *
 * @author : kqlu
 * @version :
 * @code : @since : Created in 15:40 2019/3/1
 */
public class ReadFromLocalDirectory extends ReadFromDirectory {
	private static final Logger logger = Logger.getLogger(ReadFromLocalDirectory.class);

	@Override
	public List<String> getFiles(String dir) {
		List<String> files = new LinkedList<>();
		File f = new File(dir);
		if (f.isFile()) {
			files.add(f.getAbsolutePath());
		} else if (f.isDirectory()) {
			File[] fs = f.listFiles();
			for (File tmpf : fs) {
				files.addAll(getFiles(tmpf.getAbsolutePath()));
			}
		}
		return files;
	}

	public static void main(String[] args) {
		ReadFromLocalDirectory rfld = new ReadFromLocalDirectory();
		List<String> files = rfld.getFiles("./src");
		for (String str : files) {
			logger.debug(str);
		}
	}
}
