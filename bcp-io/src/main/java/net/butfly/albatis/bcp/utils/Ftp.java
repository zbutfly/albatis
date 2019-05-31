package net.butfly.albatis.bcp.utils;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Pair;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Path;



public class Ftp implements Closeable {
    private static Logger logger = Logger.getLogger(Ftp.class);
    private final FTPClient client = new FTPClient();
    private final String base;

    public static Ftp connect() {
        String f = Configs.gets("albatis.bcp.input.ftp");
        return null == f ? null : new Ftp(new URISpec(f));
    }

    private Ftp(URISpec uri) {
        client.setControlEncoding("utf-8");
        Pair<String, Integer> s = uri.getHosts().iterator().next();
        try {
            client.connect(s.v1(), s.v2());// 连接ftp服务器
            client.login(uri.getUsername(), uri.getPassword()); // 登录ftp服务器
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        int replyCode = client.getReplyCode(); // 是否成功登录服务器
        if (!FTPReply.isPositiveCompletion(replyCode))
            logger.error("[FTP] connect [" + uri.getHost() + "] failed with code: " + replyCode);
        else logger.trace("[FTP] connect [" + uri.getHost() + "] successed.");
        base = uri.getPath();
    }

    /**
     * 上传文件
     *
     * @param remoteFilename 上传到ftp的文件名
     * @param localFile      待上传文件的名称（绝对地址） *
     * @return
     */
    public boolean uploadFile(String remoteFilename, Path localFile) {
        logger.trace("[FTP] begin [" + localFile + " -> " + remoteFilename + "]");
        boolean flag = false;
        try (InputStream inputStream = new FileInputStream(localFile.toFile())) {
            client.setFileType(FTP.BINARY_FILE_TYPE);
            CreateDirecroty(base);
            client.enterLocalPassiveMode();
            client.makeDirectory(base);
            client.changeWorkingDirectory(base);
            flag = client.storeFile(remoteFilename, inputStream);
            if (flag) logger.trace("[FTP] successfully [" + localFile + " -> " + remoteFilename + "].");
            else logger.error("[FTP] failed [" + localFile + " -> " + remoteFilename + "].");
        } catch (Exception e) {
            logger.error("[FTP] failed [" + localFile + " -> " + remoteFilename + "]", e);
        }
        return flag;
    }

    /**
     * 上传文件
     *
     * @param localPath      ftp服务保存地址
     * @param remoteFilename 上传到ftp的文件名
     * @param inputStream    输入文件流
     * @return
     */
    public boolean uploadFile(String localPath, String remoteFilename, InputStream inputStream) {
        logger.trace("[FTP] begin [" + remoteFilename + " -> " + localPath + "].");
        try {
            client.setFileType(FTP.BINARY_FILE_TYPE);
            CreateDirecroty(localPath);
            client.makeDirectory(localPath);
            client.changeWorkingDirectory(localPath);
            client.storeFile(remoteFilename, inputStream);
            inputStream.close();
            logger.trace("[FTP] successfully [" + remoteFilename + " -> " + localPath + "].");
            return true;
        } catch (Exception e) {
            logger.error("[FTP] failed [" + remoteFilename + " -> " + localPath + "].", e);
            return false;
        }
    }

    // 改变目录路径
    public boolean changeWorkingDirectory(String directory) throws IOException {
        return client.changeWorkingDirectory(directory);
    }

    // 创建多层目录文件，如果有ftp服务器已存在该文件，则不创建，如果无，则创建
    public boolean CreateDirecroty(String remote) throws IOException {
        boolean success = true;
        String directory = remote + "/";
        // 如果远程目录不存在，则递归创建远程服务器目录
        if (!directory.equalsIgnoreCase("/") && !changeWorkingDirectory(new String(directory))) {
            int start = 0;
            int end = 0;
            if (directory.startsWith("/")) {
                start = 1;
            } else {
                start = 0;
            }
            end = directory.indexOf("/", start);
            String path = "";
            String paths = "";
            while (true) {
                String subDirectory = new String(remote.substring(start, end).getBytes("GBK"), "iso-8859-1");
                path = path + "/" + subDirectory;
                if (existFile(path)) changeWorkingDirectory(subDirectory);
                else {
                    if (makeDirectory(subDirectory)) changeWorkingDirectory(subDirectory);
                    else {
                        logger.warn("dir [" + subDirectory + "] cnstruct fail ");
                        changeWorkingDirectory(subDirectory);
                    }
                }

                paths = paths + "/" + subDirectory;
                start = end + 1;
                end = directory.indexOf("/", start);
                // 检查所有目录是否创建完毕
                if (end <= start) {
                    break;
                }
            }
        }
        return success;
    }

    // 判断ftp服务器文件是否存在
    public boolean existFile(String path) throws IOException {
        return client.listFiles(path).length > 0;
    }

    // 创建目录
    public boolean makeDirectory(String dir) throws IOException {
        return client.makeDirectory(dir);
    }

    /**
     * 下载所有文件并删除原有文件
     * @param localpath 下载后的文件路径
     * @return
     */
    public boolean downloadAllFiles(String localpath) {
        logger.trace("[FTP] begin download all files [ <- " + base + "].");
        try {
            if (!existFile(base)){
                logger.trace("[FTP] path is not exist [ <- " + base + "].");
                return false;
            }
            client.changeWorkingDirectory(base);
            FTPFile[] ftpFiles = client.listFiles();
            for (FTPFile file : ftpFiles) {
                File localFile = new File(localpath + "/" + file.getName());
                try (OutputStream os = new FileOutputStream(localFile)) {
                    client.retrieveFile(file.getName(), os);
                    client.dele(file.getName());
                }
            }
            logger.trace("[FTP] download all files successfully [ <- " + base + "].");
            return true;
        } catch (IOException e) {
            logger.error("[FTP] download all files failed [<- " + base + "].", e);
            return false;
        }
    }

    /**
     * * 下载文件 *
     *
     * @param pathname  FTP服务器文件目录 *
     * @param filename  文件名称 *
     * @param localpath 下载后的文件路径 *
     * @return
     */
    public boolean downloadFile(String pathname, String filename, String localpath) {
        logger.trace("[FTP] begin [" + filename + " <- " + pathname + "].");
        try {
            // 切换FTP目录
            client.changeWorkingDirectory(pathname);
            FTPFile[] ftpFiles = client.listFiles();
            for (FTPFile file : ftpFiles) {
                if (filename.equalsIgnoreCase(file.getName())) {
                    File localFile = new File(localpath + "/" + file.getName());
                    try (OutputStream os = new FileOutputStream(localFile)) {
                        client.retrieveFile(file.getName(), os);
                    }
                }
            }
            logger.trace("[FTP] successfully [" + filename + " <- " + pathname + "].");
            return true;
        } catch (IOException e) {
            logger.error("[FTP] failed [" + filename + " <- " + pathname + "].", e);
            return false;
        }
    }

    /**
     * * 删除文件 *
     *
     * @param pathname FTP服务器保存目录 *
     * @param filename 要删除的文件名称 *
     * @return
     */
    public boolean deleteFile(String pathname, String filename) {
        logger.trace("[FTP] delete begin");
        try {
            // 切换FTP目录
            client.changeWorkingDirectory(pathname);
            client.dele(filename);
            logger.trace("[FTP] delete successed");
            return true;
        } catch (Exception e) {
            logger.error("[FTP] delete failed", e);
            return false;
        }
    }


    public void addText(String filename, String content) {
        FileWriter writer = null;
        try {
            // 打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件
            writer = new FileWriter(filename, true);
            writer.write(content);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() {
        if (client.isConnected()) try {
            client.disconnect();
        } catch (IOException e) {
        }
    }

    public static void main(String... args) {
        URISpec uri = new URISpec("ftp-user1:Hik123@172.16.17.47:21/");
        System.out.println(uri.getPath());
    }
}
