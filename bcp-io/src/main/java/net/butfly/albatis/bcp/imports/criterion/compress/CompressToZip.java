package net.butfly.albatis.bcp.imports.criterion.compress;

import net.butfly.albacore.utils.logger.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * nothing.
 *
 * @author : kqlu
 * @version :
 * @code : @since : Created in 20:18 2019/2/28
 */
public class CompressToZip {
	private static final Logger logger = Logger.getLogger(CompressToZip.class);
	private String zipFileName = "";
	private FileOutputStream fos1 = null;
	private List<File> fileList = new ArrayList<>();

	public CompressToZip() {
	}

	public void createZip(String zipFileName) throws FileNotFoundException {
		this.zipFileName = zipFileName;
		fos1 = new FileOutputStream(new File(zipFileName));
	}

	public void addFileToZip(String fileName) {
		if (fileName == null || fileName.equals("")) { return; }
		File f = new File(fileName);
		if (f.exists()) {
			fileList.add(new File(fileName));
		}
	}

	public void flush() {
		if (zipFileName.equals("")) {
			logger.debug("zipFileName is null. Please set first.");
			return;
		}
		if (fileList.size() == 0) {
			logger.debug("There is no file insert into zip:" + zipFileName + ".");
			return;
		}
		ZipUtils.toZip(fileList, fos1);
	}
}
