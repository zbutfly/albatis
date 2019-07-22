package net.butfly.albatis.file;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.io.Rmap;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static net.butfly.albacore.base.BizUnit.logger;
import static net.butfly.albacore.paral.Exeter.getn;
import static net.butfly.albacore.paral.Exeter.of;
import static net.butfly.albacore.utils.logger.StatsUtils.formatKilo;
import static net.butfly.albacore.utils.logger.StatsUtils.formatMillis;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_ZWNJ_CH;

public class FileFormat {
    public static Logger logger = Logger.getLogger(FileFormat.class);
    private static String FIELD_SPLIT = Configs.gets("dataggr.migrate.file.split", ";");
    private static final boolean ADD_FIELDS = Boolean.valueOf(Configs.gets("dataggr.migrate.file.fields", "true"));
    public static final int FTP_RETRIES = Integer.valueOf(Configs.gets("dataggr.migrate.file.ftp.retries", "3"));
    private static final AtomicInteger COUNT_LINES = new AtomicInteger();
    private static final Map<String, AtomicInteger> FILE_INDEX = new ConcurrentHashMap<>();
    private static final AtomicLong FIELD_SPENT = new AtomicLong(), REC_COUNT = new AtomicLong();
    private static final long START = System.currentTimeMillis();
    private static final AtomicLong COUNT_BCPS = new AtomicLong();
    private static final AtomicLong COUNT_CHARS = new AtomicLong();
    private String basePath;
    private String ext;
    private String format;
    private URISpec uri;
    private String ftp;

    public FileFormat(FileConnection connection) {
        super();
        this.basePath = connection.root;
        this.ext = connection.ext;
        this.format = connection.format;
        this.uri = connection.uri;
        this.ftp = connection.ftp;
        confirmDir(Paths.get(this.basePath));
    }

    public void format(List<Rmap> recs, String table, List<FieldDesc> fieldDescs) {
        String fn;
        if ("csv".equals(format)) {
            FIELD_SPLIT = ",";
        }
        if (FILE_INDEX.containsKey(table)) {
            fn = table + "-" + FILE_INDEX.get(table).incrementAndGet() + ext;
        } else {
            fn = table + "-1" + ext;
            FILE_INDEX.put(table, new AtomicInteger(1));
        }
        Path fnp = Paths.get(basePath).resolve(fn);
        List<String> lines;
        List<String> fields = getFileList(fieldDescs);
        long now = System.currentTimeMillis();
        try {
            lines = Colls.list();
            getn(Colls.list(recs, r -> of().submit(() -> {
                if ("json".equals(format) || "json:txt".equals(format)) {
                    lines.add(JsonSerder.JSON_MAPPER.ser(r));
                } else {
                    lines.add(sync(r, fieldDescs));
                }
            })));
        } finally {
            long ms = System.currentTimeMillis() - now;
            logger.debug("File [" + fn + "] for [" + recs.size() + "] rendered, spent: " + formatMillis(ms) + " ms");
        }
        if (lines.isEmpty()) return;
        writeToFile(fnp, lines, fn, fields);
    }

    private void writeToFile(Path path, List<String> lines, String fn, List<String> fields) {
        long now = System.currentTimeMillis();
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path.toFile()), "GBK"));) {
            if (ADD_FIELDS) {
                if ("json:txt".equals(format)) {
                    bw.write("jsonarrstr");
                } else {
                    bw.write(String.join(FIELD_SPLIT, fields));
                }
                bw.newLine();
            }
            lines.forEach(r -> {
                try {
                    bw.write(r);
                    bw.newLine();
                } catch (IOException e) {
                    logger.error("writeToFile IOException:"+e);
                }
            });
        } catch (UnsupportedEncodingException e) {
            logger.error("writeToFile UnsupportedEncodingException:"+e);
        } catch (FileNotFoundException e) {
            logger.error("writeToFile FileNotFoundException:"+e);
        } catch (IOException e) {
            logger.error("writeToFile IOException:"+e);
        } finally {
            if ("ftp".equals(ftp)) {
                upload(fn, path);
            }
            if (logger.isDebugEnabled()) {
                long ms = System.currentTimeMillis() - START;
                long ms0 = System.currentTimeMillis() - now;
                long cl = COUNT_LINES.addAndGet(lines.size());
                long cb = COUNT_BCPS.incrementAndGet();
                long cc = 0;
                for (String s : lines) cc = COUNT_CHARS.addAndGet(s.length());
                logger.debug("File [" + fn + "] for [" + lines.size() + "] finished, spent: " + formatMillis(ms0) + "\n\tstats: " //
                        + "[" + cb + "] files in [" + formatMillis(ms) + "], [" + cl + "] records, "
                        + "avg [" + cl * 1000.0 / ms + " recs/s] and [" + formatKilo(cc * 1000.0 / ms, "B/s") + "]");
            }
        }
    }

    protected String sync(Rmap m, List<FieldDesc> fieldDescs) {
        if (null == m.map() || m.map().isEmpty()) return null;
        long spent = System.currentTimeMillis();
        try {
            List<String> fs = new ArrayList<>();
            for (FieldDesc fd : fieldDescs) {
                count(m.map().get(fd.name), fs);
            }
            return String.join(FIELD_SPLIT, fs);
        } finally {
            spent = FIELD_SPENT.addAndGet(System.currentTimeMillis() - spent);
            long c = REC_COUNT.incrementAndGet();
            if (c % 25000 == 0) logger.trace("avg field proc time of [" + c + "] recs: " + (spent / c) + "ms/rec");
        }
    }

    private List<String> getFileList(List<FieldDesc> fieldDescs){
        List<String> fields = new ArrayList<>();
        for (FieldDesc fd : fieldDescs) {
            if (fieldDescs.size() > fields.size()) {
                fields.add(fd.name);
            }
        }
        return fields;
    }

    private void count(Object v, List<String> vs) {
        if (null == v) v = "";
        else if (v instanceof CharSequence) {
            char[] cs = ((CharSequence) v).toString().toCharArray();
            for (int i = 0; i < cs.length; i++)
                if (cs[i] == '\t' || cs[i] == '\n' || cs[i] == '\r') cs[i] = SPLIT_ZWNJ_CH;
            v = new String(cs);
        }
        vs.add(v.toString());
    }

    Path confirmDir(Path dir) {
        File file = dir.toFile();
        if (!file.exists() || !file.isDirectory()) file.mkdirs();
        return dir;
    }

    void upload(String recFilename, Path recFile) {
        boolean flag;
        int retry = 0;
        try (Ftp ftp = Ftp.connect(uri)) {
            if (null != ftp) {
                while (!(flag = ftp.uploadFile(recFilename, recFile)) && retry++ < FTP_RETRIES) ;
                if (!flag && retry >= FTP_RETRIES) logger.error("Ftp sent failed: " + recFile);
            }
        }
    }
}
