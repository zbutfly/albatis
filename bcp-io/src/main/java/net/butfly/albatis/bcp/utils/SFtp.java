package net.butfly.albatis.bcp.utils;

import static net.butfly.albatis.bcp.Props.CLEAN_TEMP_FILES;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Properties;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Pair;
import net.butfly.albatis.bcp.Props;

public class SFtp implements Closeable {
	private transient Logger LOGGER = LoggerFactory.getLogger(this.getClass());
	private ChannelSftp sftp;
	private Session session;
	private final String base;
	public final static HashMap<String, FileStatus> fileStatusMap = new HashMap<>();
	private long localTime;

	public static SFtp connect(URISpec uri, String varPath) {
		return null == uri || uri.toString().contains("///") ? null : new SFtp(uri, varPath);
	}

	/**
	 * 连接sftp服务器
	 */
	private SFtp(URISpec uri, String varPath) {
		base = Paths.get(uri.getPath() + varPath).toString();
		localTime = new java.util.Date().getTime();
		try {
			JSch jsch = new JSch();
			Pair<String, Integer> s = uri.getHosts().iterator().next();
			String password = uri.getPassword();
			// if (null != password) jsch.addIdentity(password);// 设置私钥 privateKey
			session = jsch.getSession(uri.getUsername(), s.v1(), s.v2());
			if (null != password) session.setPassword(password);
			Properties config = new Properties();
			config.put("StrictHostKeyChecking", "no");

			session.setConfig(config);
			session.connect();

			Channel channel = session.openChannel("sftp");
			channel.connect();
			sftp = (ChannelSftp) channel;
		} catch (JSchException e) {
			LOGGER.error("sftp connect error", e);
		}
	}

	/**
	 * 上传文件
	 *
	 * @param remoteFilePath
	 *            上传到ftp的文件路径
	 * @param localFile
	 *            待上传文件的名称（绝对地址） *
	 * @return
	 */
	public boolean uploadFile(String remoteFilePath, Path localFile) {
		LOGGER.trace("[SFTP] begin [" + localFile + " -> " + remoteFilePath + "]");
		try (InputStream inputStream = new FileInputStream(localFile.toFile())) {
			Path remotePath = Paths.get(remoteFilePath).getParent();
			createDir(remotePath.toString(), sftp);
			sftp.put(inputStream, remoteFilePath);
			LOGGER.trace("[SFTP] successfully [" + localFile + " -> " + remoteFilePath + "].");
			return true;
		} catch (Exception e) {
			LOGGER.error("[SFTP] failed [" + localFile + " -> " + remoteFilePath + "]", e);
		}
		return false;
	}

	/**
	 * 创建一个文件目录
	 */
	public void createDir(String createPath, ChannelSftp sftp) {
		try {
			if (isDirExist(createPath)) {
				sftp.cd(createPath);
				return;
			}
			String[] pathArray = createPath.split("/");
			StringBuilder filePath = new StringBuilder("/");
			for (String path : pathArray) {
				if (path.equals("")) {
					continue;
				}
				filePath.append(path).append("/");
				if (isDirExist(filePath.toString())) {
					sftp.cd(filePath.toString());
				} else {
					// 建立目录
					sftp.mkdir(filePath.toString());
					// 进入并设置为当前目录
					sftp.cd(filePath.toString());
				}
			}
			sftp.cd(createPath);
		} catch (SftpException e) {
			LOGGER.error("create path error:" + createPath);
		}
	}

	/**
	 * 判断目录是否存在
	 */
	public boolean isDirExist(String directory) {
		boolean isDirExistFlag = false;
		try {
			SftpATTRS sftpATTRS = sftp.lstat(directory);
			isDirExistFlag = true;
			return sftpATTRS.isDir();
		} catch (Exception e) {
			if (e.getMessage().toLowerCase().equals("no such file")) {
				isDirExistFlag = false;
			}
		}
		return isDirExistFlag;
	}

	public void downloadAllFiles(String localPath) {
		try {
			// sftp.setFilenameEncoding("utf-8");
			Props.confirmDir(Paths.get(localPath));
			sftp.cd(base);
			Vector files = sftp.ls("*");
			FileStatus fileStatus = null;
			for (int i = 0; i < files.size(); i++) {
				Object obj = files.elementAt(i);
				if (obj instanceof com.jcraft.jsch.ChannelSftp.LsEntry) {
					ChannelSftp.LsEntry entry = (ChannelSftp.LsEntry) obj;
					SftpATTRS attrs = entry.getAttrs();
					int time = attrs.getMTime();
					long size = attrs.getSize();
					String fileName = entry.getFilename();
					fileStatus = fileStatusMap.get(fileName);
					if (null == fileStatus) {
						fileStatus = new FileStatus();
						fileStatus.fileName = fileName;
						fileStatus.lastTime = localTime;
						fileStatus.processedFlag = 0;
						fileStatus.timestamp = time;
						fileStatus.size = size;
						fileStatusMap.put(fileName, fileStatus);
						continue;
					} else if (fileStatus.processedFlag != 0) {
						continue;
					} else if (localTime - fileStatus.lastTime < Props.BCP_FILE_LISTEN_TIME) {
						fileStatus.timestamp = time;
						fileStatus.size = size;
						continue;
					} else if (fileStatus.size != size || fileStatus.timestamp != time) {
						fileStatus.lastTime = localTime;
						fileStatus.timestamp = time;
						fileStatus.size = size;
						continue;
					}
					Props.confirmDir(Paths.get(localPath));
					File localFile = new File(localPath + "/" + fileName);
					try (OutputStream os = new FileOutputStream(localFile)) {
						sftp.get(fileName, os);
						if (CLEAN_TEMP_FILES) delete(base, fileName);
						LOGGER.trace("[SFTP] download  file [ " + fileName + "] successfully [ <- " + base + "].");
					}
				}
			}
		} catch (SftpException | IOException e) {
			LOGGER.error("[SFTP] download  file failed.", e);
		}
	}

	/**
	 * 删除文件
	 *
	 * @param directory
	 *            要删除文件所在目录
	 * @param deleteFile
	 *            要删除的文件
	 */
	public void delete(String directory, String deleteFile) throws SftpException {
		sftp.cd(directory);
		sftp.rm(deleteFile);
	}

	/**
	 * 关闭连接 server
	 */
	@Override
	public void close() {
		if (sftp != null && sftp.isConnected()) sftp.disconnect();
		if (session != null && session.isConnected()) session.disconnect();
	}

	public static void main(String[] args) {
		try (SFtp sFtp = new SFtp(new URISpec("bcp:csv:sftp://demo:demo@10.192.78.29:55555/demo"), "/cascade");) {
			ChannelSftp sftp = sFtp.sftp;
			sftp.disconnect();
		}
	}
}