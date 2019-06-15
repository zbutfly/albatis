package net.butfly.albatis.bcp.imports.criterion;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.bcp.Props;
import net.butfly.albatis.bcp.imports.criterion.compress.CompressToZip;
import net.butfly.albatis.bcp.imports.criterion.writer.WriteToBcp;
import net.butfly.albatis.bcp.imports.criterion.writer.WriteToXml;
import net.butfly.albatis.bcp.imports.frame.struct.KernelInfo;
import net.butfly.albatis.bcp.utils.Ftp;
import net.butfly.albatis.bcp.utils.MD5Util;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

import static net.butfly.albatis.bcp.Props.CLEAN_TEMP_FILES;


/**
 * nothing.
 *
 * @author : kqlu
 * @version :
 * @code : @since : Created in 16:27 2019/3/1
 */
public class Criterion implements AutoCloseable {
	private static final Logger logger = Logger.getLogger(Criterion.class);
	private static final String XML_NAME = "GAB_ZIP_INDEX.xml";
	private WriteToXml writeToXml = null;
	private WriteToBcp writeToBcp = null;
	// 输入信息
	private final String lineSplit;
	private final String fieldSplit;
	// 输出数据源名称
	private final String dataEName;
	private final String outputDir;
	private final String outputDirTmp;
	private String zipDst = "";

	// 0 zip ； 1 bcp ；2 SystemOutput
	private final int runMode;
	// 保存字段是否需要输出
	private boolean[] outoutArray = null;
	// 输出字段元数据信息
	private String[][] fieldAndComments = null;

	// 控制数据输出比例
	private int outRate = 100;
	private int tmpCount = 0;
	private int PERSENT = 100;

	private int callWriteCount = 0;
	// 控制文件分隔数
	private int FILEMAXNUM = 10000;
	private Path endPath = Props.BCP_PATH_BASE.resolve("zip");
	private URISpec uri;

	public Criterion(KernelInfo kernelInfo, URISpec uri) {
		this.uri = uri;
		lineSplit = kernelInfo.getIntputLineSplit();
		fieldSplit = kernelInfo.getIntputFiledSplit();

		dataEName = kernelInfo.getInputDataEName();
		outputDir = kernelInfo.getOutputPath();
		outputDirTmp = outputDir + "_tmp";

		FileUtils.mkdir(outputDir);
		FileUtils.cleanDir(outputDir);
		FileUtils.mkdir(outputDirTmp);
		FileUtils.cleanDir(outputDirTmp);

		// 记录数据是否需要输出
		outoutArray = new boolean[kernelInfo.getFields().size()];
		// 数据元数据信息
		int tmpCount = 0;
		for (int i = 0; i < outoutArray.length; i++) {
			outoutArray[i] = !kernelInfo.getFields().get(i).getDestField().equals("");
			if (outoutArray[i]) tmpCount++;
		}
		// 记录数据的元数据信息
		fieldAndComments = new String[tmpCount][2];
		tmpCount = 0;
		for (int i = 0; i < outoutArray.length; i++) if (outoutArray[i]) {
			fieldAndComments[tmpCount][0] = kernelInfo.getFields().get(i).getDestField();
			fieldAndComments[tmpCount++][1] = kernelInfo.getFields().get(i).getComment();
		}

		// 数据输出比例
		outRate = kernelInfo.getOutputRate();
		// 数据输出类型
		switch (kernelInfo.getOutputType()) {
		case "zip":
			runMode = 0;
			initXml();
			initBcp();
			break;
		case "bcp":
			runMode = 1;
			initBcp();
			break;
		default:
			runMode = 2;
			break;
		}
	}

	private void initXml() {
		if (writeToXml != null) {
			writeToXml.addBcpInfo("", writeToBcp.getFileName(), String.valueOf(callWriteCount));
			writeToXml.flush();
		} else {
			writeToXml = new WriteToXml(outputDirTmp + "/" + XML_NAME);
			writeToXml.addDataSourceInfo("fenghuo", "330000", "999", "UTF-8", lineSplit, fieldSplit);
			writeToXml.addFileds(fieldAndComments);
		}
	}

	private void initBcp() {
		if (writeToBcp != null) {
			writeToBcp.close();
		} else {
			int se = (int) (System.currentTimeMillis() / 1000);
			String mi = String.format("%05d", System.currentTimeMillis() % 1000);
			String bcpName = "999-330000-" + se + "-" + mi + "-" + dataEName + "-0.bcp";
			writeToBcp = new WriteToBcp(outputDirTmp + "/" + bcpName, "\t", "UTF-8");
		}
	}

	// 返回要输出的记录数
	private String[] outputFilter(String[] intputs) {
		List<String> tmpList = new LinkedList<>();
		for (int i = 0; i < outoutArray.length; i++) if (outoutArray[i]) tmpList.add(intputs[i]);
		return (String[]) tmpList.toArray(new String[0]);
	}

	public void write(String[] fileds) {
		tmpCount = (tmpCount + 1) % PERSENT;
		if (tmpCount > outRate) { return; }

		String[] outputs = outputFilter(fileds);
		if (outputs == null) { return; }

		switch (runMode) {
		case 0:
			if (callWriteCount >= FILEMAXNUM) {
				close();
				// 重建相关文件
				initXml();
				initBcp();
				callWriteCount = 0;
			}
			callWriteCount++;
			writeToBcp.write(fileds);
			break;
		case 1:
			if (callWriteCount >= FILEMAXNUM) {
				// writeToBcp.close();
				close();
				initBcp();
				callWriteCount = 0;
			}
			callWriteCount++;
			writeToBcp.write(fileds);
			break;
		case 2:
			StringBuilder sb = new StringBuilder();
			for (String str : fileds) {
				sb.append(str).append("\t");
			}
			logger.debug(sb.toString());
			break;
		}
	}

	@Override
	public void close() {
		if (writeToXml != null) {
			writeToXml.addBcpInfo("", getBcpFileName(writeToBcp.getFileName()), String.valueOf(callWriteCount));
			writeToXml.flush();
		}
		if (writeToBcp != null) writeToBcp.close();
		switch (runMode) {
		case 0:
			zip();
			break;
		case 1:
			FileUtils.move(outputDir, outputDirTmp);
			break;
		default:
			break;

		}
		writeToXml = null;
		writeToBcp = null;
	}

	private void zip() {
		Path zipSrc = getZipFileName(writeToBcp.getFileName());
		CompressToZip compressToZip = new CompressToZip();
		try {
			compressToZip.createZip(zipSrc.toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
		if (logger.isTraceEnabled()) {
			long xmlsize = new File(writeToXml.getOutputFileName()).length(), bcpsize = new File(writeToBcp.getFileName()).length();
			logger.trace("zip: [" + zipSrc + "] from \n\t" //
					+ "bcp: [" + writeToBcp.getFileName() + "][" + xmlsize + " bytes], \n\t"//
					+ "xml: [" + writeToBcp.getFileName() + "][" + bcpsize + " bytes].");
		}
		compressToZip.addFileToZip(writeToXml.getOutputFileName());
		compressToZip.addFileToZip(writeToBcp.getFileName());
		compressToZip.flush();
		String fileMD5 = MD5Util.getFileMd5(writeToBcp.getFileName());
		if(CLEAN_TEMP_FILES){
			FileUtils.cleanDir(outputDirTmp);
		}
		if (logger.isTraceEnabled()) {
			long zs = zipSrc.toFile().length();
			logger.trace("zip: [" + zipSrc + "][" + zs + " bytes] generated.");
		}

		boolean flag;
		int n = 0;
		try (Ftp ftp = Ftp.connect(uri)) {
			if (null != ftp) {
				while (!(flag = ftp.uploadFile(zipDst, zipSrc)) && n++ < 5);
				if (flag) {
					String dir = dataEName.substring(0, dataEName.indexOf("-"));
					ftp.moveTotherFolders(zipSrc, zipDst, dir);
					// 上传对应文件名称,MD5,数据量
					String bcpFileName = writeToBcp.getFileName();
					int index2 = bcpFileName.lastIndexOf("/");
					String bcpName = bcpFileName.substring(index2 + 1, writeToBcp.getFileName().length());
					String content = "bcpFileName:" + bcpName + "\tfileMD5:" + fileMD5 + "\tcount:" + callWriteCount;
					String txtname = bcpName + ".txt";
					Path txt = endPath.resolve(dir).resolve(txtname);
					ftp.addText(txt.toString(), content);
					ftp.uploadFile(txtname, txt);

				} else logger.error("zip [" + zipDst + "] transfer fail.");
			}
		}
		// 清理本地临时文件
		if(CLEAN_TEMP_FILES){
			String deName = dataEName;
			FileUtils.deleteDirectory(Props.BCP_PATH_BASE + File.separator + deName.substring(0, dataEName.lastIndexOf("-")) + File.separator + deName);
			logger.info("清理本地临时文件:"+Props.BCP_PATH_BASE + File.separator + deName.substring(0, dataEName.lastIndexOf("-")) + File.separator + deName);
		}
	}

	private Path getZipFileName(String bcpFileName) {
		int index = bcpFileName.lastIndexOf(".");
		int index2 = bcpFileName.lastIndexOf("/");
		zipDst = bcpFileName.substring(index2 + 1, index) + ".zip";
		return Paths.get(outputDir).resolve(zipDst);
	}

	private String getBcpFileName(String bcpFileName) {
		int index2 = bcpFileName.lastIndexOf("/");
		return bcpFileName.substring(index2 + 1);
	}
}
