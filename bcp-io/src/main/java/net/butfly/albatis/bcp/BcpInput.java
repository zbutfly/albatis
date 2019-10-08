package net.butfly.albatis.bcp;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.bcp.utils.*;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Rmap;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static net.butfly.albatis.bcp.Props.CLEAN_TEMP_FILES;


public class BcpInput extends Namedly implements Input<Rmap> {
    private static final long serialVersionUID = -8772607845694039875L;
    private static final String FIELD_SPLIT = "\t";
    private static final String ZIP = "zip";
    private static final String BCP = "bcp";
    private static final String GAB_ZIP_INDEX = "GAB_ZIP_INDEX.xml";
    private final LinkedBlockingQueue<Bcp> BCP_POOL = new LinkedBlockingQueue<>(20000);
    private String pathTable;
    private String tableName;
    private String varPath = "";
    private Path dataPath;
    private Path bcpPath;
    private List<String> zipNames;
    private int count;
    private URISpec uri;

    public BcpInput(final String name, URISpec uri, String dataPath, String table) {
        super(name);
        this.pathTable = table;
        this.tableName = getTableName(table); // path + tableName
        this.dataPath = getDataPath(dataPath, table);// varPath
        this.bcpPath = this.dataPath.resolve(getTableName(table));
        FileUtil.confirmDir(this.bcpPath);
        this.uri = uri;
        FtpDownloadThread thread = new FtpDownloadThread();
        new Thread(thread).start();
        closing(this::closeBcp);
    }

    private String getTableName(String tableName) {
        if (tableName.contains("/")) return tableName.substring(tableName.lastIndexOf("/") + 1);
        else return tableName;
    }

    private Path getDataPath(String dataPath, String tableName) {
        File directory = new File(this.getClass().getResource("/").getPath());
        if (tableName.contains("/")) varPath = tableName.substring(0, tableName.lastIndexOf("/") + 1);
        String parent = directory.getParent();
        return Paths.get(parent + dataPath + varPath);
    }

    private void download() {
        try (Ftp ftp = Ftp.connect(uri, varPath)) {
            if (null != ftp) {
                ftp.downloadAllFiles(dataPath.toString());
            }
        }
    }

    private void openBcp() {
        File localFile = dataPath.toFile();
        File[] files = localFile.listFiles();
        FileStatus fileStatus = null;
        if (null != files) {
            for (File file : files) {
                String fileName = file.getName();
                long localTime = new Date().getTime();
                fileStatus = Ftp.fileStatusMap.get(fileName);
                if (null == fileStatus) {
                    fileStatus = new FileStatus();
                    fileStatus.fileName = fileName;
                    fileStatus.lastTime = localTime;
                    fileStatus.processedFlag = 0;
                    fileStatus.timestamp = file.lastModified();
                    fileStatus.size = file.length();
                    Ftp.fileStatusMap.put(fileName, fileStatus);
                    continue;
                } else if (fileStatus.processedFlag != 0) {
                    continue;
                } else if (localTime - fileStatus.lastTime < 30000) {
                    fileStatus.timestamp = file.lastModified();
                    fileStatus.size = file.length();
                    continue;
                } else if (fileStatus.size != file.length() || fileStatus.timestamp != file.lastModified()) {
                    fileStatus.lastTime = localTime;
                    fileStatus.timestamp = file.lastModified();
                    fileStatus.size = file.length();
                    continue;
                }
            }
        }
        if (uri.getSchemas().length >= 2 && !uri.getSchemas()[1].equals("ftp"))
            zipNames = FileUtil.getFileNames(uri.getSchemas()[1], tableName, dataPath.toString());
        else
            zipNames = FileUtil.getFileNames(ZIP, tableName, dataPath.toString());
        zipNames.forEach(zipName -> {
            if (uri.getSchemas().length >= 2 && !uri.getSchemas()[1].equals("ftp")) {
                File file = new File(zipName);
                String renameFile = zipName.replace("." + uri.getSchemas()[1], ".zip");
                file.renameTo(new File(renameFile));
                new Bcp(renameFile);
            } else
                new Bcp(zipName);
        });
    }


    private void closeBcp() {
        Bcp bcp;
        while (Props.BCP_STOP_FLAG == 0) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        while (count != 0) {
            if (null != (bcp = BCP_POOL.poll())) bcp.close();
        }
        FileUtil.deleteDirectory(bcpPath.toString());
    }


    @Override
    public void dequeue(net.butfly.albacore.io.lambda.Consumer<Sdream<Rmap>> using) {
        Bcp bcp;
        while (Props.BCP_STOP_FLAG == 0 || (opened() && count != 0)) {
            if (null != (bcp = BCP_POOL.poll())) {
                try {
                    List<Rmap> rmaps = FileUtil.loadBcpData(bcp.fields, FIELD_SPLIT, bcp.bcps, pathTable);
                    if (!rmaps.isEmpty()) {
                        using.accept(Sdream.of(rmaps));
                    }
                    bcp.close();
                } catch (IllegalStateException ex) {
                    continue;
                }
            }
        }
    }


    private class Bcp {
        String zipName;
        List<String> bcps;
        List<String> fields;
        Path tempPath;

        private Bcp(String zipName) {
            super();
            this.zipName = zipName;
            String fileName = zipName.substring(zipName.lastIndexOf(File.separator) + 1);
            tempPath = bcpPath.resolve(fileName.substring(0, fileName.length() - 4));
            FileUtil.confirmDir(tempPath);
            UnZip.unZip(zipName, tempPath);
            fields = XmlUtil.readXml(tempPath.resolve(GAB_ZIP_INDEX).toString());
            bcps = FileUtil.getFileNames(BCP, tableName, tempPath.toString());
            BCP_POOL.add(this);
        }

        private void close() {
            count--;
            FileUtil.deleteZip(dataPath.toString());
            if (CLEAN_TEMP_FILES) FileUtil.deleteDirectory(tempPath.toString());
        }
    }

    private class FtpDownloadThread implements Runnable {
        @Override
        public void run() {
            while (Props.BCP_STOP_FLAG == 0) {
                download();
                openBcp();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


}
