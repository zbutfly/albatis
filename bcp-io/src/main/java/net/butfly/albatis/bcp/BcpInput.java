package net.butfly.albatis.bcp;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.bcp.utils.FileUtil;
import net.butfly.albatis.bcp.utils.Ftp;
import net.butfly.albatis.bcp.utils.UnZip;
import net.butfly.albatis.bcp.utils.XmlUtil;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Rmap;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;



public class BcpInput extends Namedly implements Input<Rmap> {
    private static final long serialVersionUID = -8772607845694039875L;
    private static final String FIELD_SPLIT = "\t";
    private static final String ZIP = "zip";
    private static final String BCP = "bcp";
    private static final String GAB_ZIP_INDEX = "GAB_ZIP_INDEX.xml";
    private final LinkedBlockingQueue<Bcp> BCP_POOL = new LinkedBlockingQueue<>(20000);
    private String tableName;
    private Path dataPath;
    private Path bcpPath;
    List<String> zipNames;

    public BcpInput(final String name, URISpec uri, String dataPath, String tableName) {
        super(name);
        this.tableName = tableName;
        this.dataPath = Paths.get(dataPath);
        this.bcpPath = this.dataPath.resolve(tableName);
        FileUtil.confirmDir(this.bcpPath);
        opening(() -> openBcp(uri) );
        closing(this::closeBcp);
    }

    private void openBcp(URISpec uri) {
        try (Ftp ftp = Ftp.connect(uri)) {
            if (null != ftp) {
                ftp.downloadAllFiles(dataPath.toString());
            }
        }
        zipNames = FileUtil.getFileNames(ZIP, tableName, dataPath.toString());
        zipNames.forEach(zipName -> {
            new Bcp(zipName);
        });
    }

    private void closeBcp() {
        Bcp bcp;
        while (!zipNames.isEmpty()){
            if (null != (bcp = BCP_POOL.poll())) bcp.close();
        }
        FileUtil.deleteDirectory(bcpPath.toString());
    }

    @Override
    public boolean empty() {
        return zipNames.isEmpty();
    }


    @Override
    public void dequeue(net.butfly.albacore.io.lambda.Consumer<Sdream<Rmap>> using) {
        Bcp bcp;
        while (opened() && !empty()) {
            if (null != (bcp = BCP_POOL.poll())) {
                try {
                    List<Rmap> rmaps = FileUtil.loadBcpData(bcp.fields, FIELD_SPLIT, bcp.bcps, tableName);
                    if (!rmaps.isEmpty()) {
                        using.accept(Sdream.of(rmaps));
                    }
                    bcp.close();
                } catch (IllegalStateException ex) {
                    continue;
                }
            }
        }
        return;
    }


    private class Bcp {
        String zipName;
        List<String> bcps;
        List<String> fields;
        Path tempPath;

        private Bcp(String zipName) {
            super();
            this.zipName = zipName;
            String fileName = zipName.substring(zipName.lastIndexOf(File.separator)+1);
            tempPath = bcpPath.resolve(fileName.substring(0, fileName.length() - 4));
            FileUtil.confirmDir(tempPath);
            UnZip.unZip(zipName, tempPath);
            fields = XmlUtil.readXml(tempPath.resolve(GAB_ZIP_INDEX).toString());
            bcps = FileUtil.getFileNames(BCP, tableName, tempPath.toString());
            BCP_POOL.add(this);
        }

        private void close() {
            zipNames.remove(zipName);
            FileUtil.deleteDirectory(tempPath.toString());
            FileUtil.deleteZip(dataPath.toString());
        }
    }

}
