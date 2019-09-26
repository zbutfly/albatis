package net.butfly.albatis.bcp;

import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.TableDesc;

import java.nio.file.Path;
import java.util.Date;
import java.util.List;

import static net.butfly.albatis.bcp.Props.*;


public class TaskDesc {
    public final String dstTableName;
    public final List<FieldDesc> fields = Colls.list();
    public final FileDesc fd;


    public TaskDesc(TableDesc tableDesc) {
        this.dstTableName = getTableName(tableDesc);
        assembleFields(tableDesc);
        this.fd = new FileDesc();
    }

    private String getTableName(TableDesc tableDesc) {
        String tableName = tableDesc.qualifier.name;
        if (tableName.contains("/")) return tableName.substring(tableName.lastIndexOf("/") + 1);
        else return tableName;
    }

    private void assembleFields(TableDesc tableDesc) {
        List<net.butfly.albatis.ddl.FieldDesc> fieldDescList = tableDesc.fieldDescList;
        fieldDescList.forEach(fieldDesc -> {
            FieldDesc fd = new FieldDesc(fieldDesc.name);
            fields.add(fd);
        });
    }

    public static class FieldDesc {
        public final String dstName;

        FieldDesc(String dstFieldName) {
            super();
            this.dstName = dstFieldName;
        }

        @Override
        public String toString() {
            return dstName + ":name=" + dstName;
        }
    }

    public class FileDesc {
        public final Path base;
        private final Path recFile;
        private final String recFilename;

        FileDesc() {
            super();
            this.recFilename = dstTableName + "-" + Texts.formatDate("yyyyMMddhhmmss", new Date()) + ".log";
            this.recFile = BCP_PATH_BASE.resolve(recFilename);
            this.base = confirmDir(BCP_PATH_BASE.resolve(dstTableName));
            confirmDir(BCP_PATH_ZIP.resolve(dstTableName));
        }

//        public void rec(String fn, int count, Map<String, Integer> fieldCounts) throws IOException {
//            String info = "表名-序号:" + fn + " 对应ZIP记录数:" + count + " 记录时间:" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
//            try (FileWriter w = new FileWriter(recFile.toFile(), true); PrintWriter p = new PrintWriter(w);) {
//                p.println(info + "\n" + fieldCounts.toString());
//            }
//            Exeter.of().submit(() -> {
//                upload(Props.FTP_RETRIES);
//            });
//        }
//
//        void upload(int retries) {
//            boolean flag;
//            int retry = 0;
//            URISpec uriSpec = new URISpec(dstDsUri);
//            String path = uriSpec.getPath();
//            File file = new File(path);
//            if (!file.exists()) {
//                file.mkdirs();
//            }
//            try (Ftp ftp = Ftp.connect(uriSpec)) {
//                if (null != ftp) {
//                    while (!(flag = ftp.uploadFile(recFilename, recFile)) && retry++ < retries) ;
//                    if (!flag && retry >= retries) logger.error("Ftp sent failed: " + recFile);
//                }
//            }
//        }
    }
}
