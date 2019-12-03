package net.butfly.albatis.bcp;

import static net.butfly.albacore.paral.Exeter.get;
import static net.butfly.albacore.paral.Exeter.getn;
import static net.butfly.albacore.paral.Exeter.of;
import static net.butfly.albacore.utils.logger.StatsUtils.formatKilo;
import static net.butfly.albacore.utils.logger.StatsUtils.formatMillis;
import static net.butfly.albatis.bcp.Props.ENCODING;
import static net.butfly.albatis.bcp.Props.FIELD_SPLIT;
import static net.butfly.albatis.bcp.Props.confirmDir;
import static net.butfly.albatis.ddl.FieldDesc.SPLIT_ZWNJ_CH;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.dom4j.DocumentException;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.bcp.imports.trans.TransToZIP;
import net.butfly.albatis.bcp.imports.trans.WriteToNb;
import net.butfly.albatis.bcp.imports.trans.WriteToXml;
import net.butfly.albatis.io.Rmap;

public class BcpFormat {
    public static Logger logger = Logger.getLogger(BcpFormat.class);
    private static final AtomicInteger fileIndex = new AtomicInteger();
    // ForkJoinPool.commonPool()
    // Exeter.of()
    // forkjoin pool with async(true) for performance and realiability
    // private final static ExecutorService exec = Executors.newCachedThreadPool();
    // new ForkJoinPool(Props.HTTP_PARAL, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);

    final List<TaskDesc> tasks;

    public BcpFormat(List<TaskDesc> tasks) {
        super();
        this.tasks = tasks;
    }

    private static final AtomicLong COUNT_LINES = new AtomicLong();
    private static final AtomicLong COUNT_BCPS = new AtomicLong();
    private static final AtomicLong COUNT_CHARS = new AtomicLong();
    private static final long START = System.currentTimeMillis();

    public void bcp(List<Rmap> recs, URISpec uri, String pathTable, String tmpDir) {
        if (recs.isEmpty()) return;
        String tableName = "";
        String varPath = "";
        if (pathTable.contains("/")) {
            tableName = pathTable.substring(pathTable.lastIndexOf("/") + 1);
            varPath = pathTable.substring(0, pathTable.lastIndexOf("/") + 1);
        }else tableName = pathTable;
        String finalTable = tableName;
        List<TaskDesc> taskDescs = tasks.stream().filter(task -> finalTable.equals(task.dstTableName)).collect(Collectors.toList());
        if(0 == taskDescs.size()) throw new RuntimeException("BcpOutput have no obtain task describe information");
        TaskDesc taskDesc = taskDescs.get(0);
        String dstTableName = taskDesc.dstTableName;
        String fn = dstTableName + "-" + fileIndex.incrementAndGet();
        logger.trace("BCP [" + fn + "] for [" + recs.size() + "] recs beginning... ");
        Map<String, Integer> counts = Maps.of();
        Path localPath;
        if (uri.toString().contains("///"))
            localPath = confirmDir(confirmDir(Paths.get(uri.getPath() + varPath).resolve(dstTableName)).resolve(fn)).resolve(tmpDir);
        else {
            localPath = confirmDir(Paths.get(taskDesc.fd.base.toString()).resolve(fn)).resolve(tmpDir);
            Props.ftpPath = Paths.get(uri.getPath() + varPath);
        }
        confirmDir(localPath);
        List<String> lines;
        long now = System.currentTimeMillis();
        try {
            lines = Colls.list();
//            getn(Colls.list(recs, r -> of().submit(() -> {
//                lines.add(sync(r, counts, taskDesc));
//            })));
            recs.parallelStream().forEach(r -> lines.add(sync(r, counts, taskDesc)));
        } finally {
            long ms = System.currentTimeMillis() - now;
            logger.debug("BCP [" + fn + "] for [" + recs.size() + "] rendered, spent: " + formatMillis(ms) + " ms");
        }
        if (lines.isEmpty()) return;
        try {
//            of().submit(() -> {
//                try {
//                    bcp(uri, localPath, fn, lines, taskDesc);
//                } catch (IOException e) {
//                    logger.error("BCP [" + fn + "] for [" + recs.size() + "] failed", e);
//                }
//            }).get();
            bcp(uri, localPath, fn, lines, taskDesc);
        } catch (Exception e) {
            logger.error("error!", e);
        }
//        of().submit(() -> {
//            try {
//                taskDesc.fd.rec(fn, lines.size(), counts);
//            } catch (IOException e) {
//                logger.error("BCP [" + fn + "] for [" + recs.size() + "] rec log fail", e);
//            }
//        });
    }

    protected void bcp(URISpec uri, Path localPath, String fn, List<String> lines, TaskDesc task) throws IOException {
        long now = System.currentTimeMillis();
        xml(localPath, fn, task);
        nb(localPath, fn, lines);
//        Future<?> f1 = of().submit(() -> {
//            xml(localPath, fn, task);
//        });
//        Future<?> f2 = of().submit(() -> {
//            nb(localPath, fn, lines);
//        });
//        get(f1, f2);
        zip(localPath, uri, task.dstTableName + "_" + UUID.randomUUID().toString());
        if (logger.isDebugEnabled()) {
            long ms = System.currentTimeMillis() - START;
            long ms0 = System.currentTimeMillis() - now;
            long cl = COUNT_LINES.addAndGet(lines.size());
            long cb = COUNT_BCPS.incrementAndGet();
            long cc = 0;
            for (String s : lines) cc = COUNT_CHARS.addAndGet(s.length());
            logger.debug("BCP [" + fn + "] for [" + lines.size() + "] finished, spent: " + formatMillis(ms0) + "\n\tstats: " //
                    + "[" + cb + "] bcps in [" + formatMillis(ms) + "], [" + cl + "] records, "//
                    + "avg [" + cl * 1000.0 / ms + " recs/s] and [" + formatKilo(cc * 1000.0 / ms, "B/s") + "]");
        }
    }

    private static final AtomicLong FIELD_SPENT = new AtomicLong(), REC_COUNT = new AtomicLong();

    protected String async(Map<String, Object> m, Map<String, Integer> counts, TaskDesc task) {
        if (null == m || m.isEmpty()) return null;
        long spent = System.currentTimeMillis();
        try {
            Map<String, Object> fs = Maps.of(); // future or value
            for (TaskDesc.FieldDesc fd : task.fields) {
                Object v;
//                if (null == fd.dstExpr) v = m.get(fd.fieldName);
//                else v = of().submit(() -> {
//                    Engine.eval(fd.dstExpr, m);
//                });
//                if (null != v) fs.put(fd.dstName, v);
            }
            List<String> vs = new ArrayList<>();
            fs.forEach((k, r) -> count(k, future(r), vs, counts));
            return String.join(FIELD_SPLIT, vs);
        } finally {
            spent = FIELD_SPENT.addAndGet(System.currentTimeMillis() - spent);
            long c = REC_COUNT.incrementAndGet();
            if (c % 25000 == 0) logger.trace("avg field proc time of [" + c + "] recs: " + (spent / c) + "ms/rec");
        }
    }

    @SuppressWarnings("unchecked")
    private Object future(Object r) {
        if (null == r) return null;
        if (r instanceof Future) try {
            return ((Future<String>) r).get();
        } catch (ExecutionException e) {
            logger.error("Expr fail", e.getCause());
            return null;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        else return r;
    }

    protected String sync(Rmap m, Map<String, Integer> counts, TaskDesc task) {
        if (null == m.map() || m.map().isEmpty()) return null;
        long spent = System.currentTimeMillis();
        try {
            List<String> fs = new ArrayList<>();
            for (TaskDesc.FieldDesc fd : task.fields)
                count(fd.dstName, m.map().get(fd.dstName), fs, counts);
            return String.join(FIELD_SPLIT, fs);
        } finally {
            spent = FIELD_SPENT.addAndGet(System.currentTimeMillis() - spent);
            long c = REC_COUNT.incrementAndGet();
            if (c % 25000 == 0) logger.trace("avg field proc time of [" + c + "] recs: " + (spent / c) + "ms/rec");
        }
    }

    private void count(String k, Object v, List<String> vs, Map<String, Integer> counts) {
        if (null == v) v = "";
        else if (v instanceof CharSequence) {
            char[] cs = ((CharSequence) v).toString().toCharArray();
            for (int i = 0; i < cs.length; i++)
                if (cs[i] == '\t' || cs[i] == '\n' || cs[i] == '\r') cs[i] = SPLIT_ZWNJ_CH;
            v = new String(cs);
        } else counts.compute(k, (f, origin) -> (null == origin ? 0 : origin.intValue()) + 1);
        vs.add(v.toString());
    }

    /**
     * zip the dir "xml" into "zip"
     */
    private void zip(Path base, URISpec uri, String table) throws IOException {
        confirmDir(base.resolve("zip"));
        try {
            TransToZIP.ZIP(base.resolve("xml").toString(), uri, table);
        } catch (DocumentException e) {
            throw new IOException(e);
        }
    }

    /**
     * generate "base/nb/fn.nb"
     */
    private void nb(Path base, String filename, List<String> lines) {
        confirmDir(base.resolve("nb"));
        try (WriteToNb wnb = new WriteToNb(base.resolve("nb").resolve(filename + ".nb").toString(), ENCODING)) {
            wnb.write(lines.toArray(new String[0]));
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * generate "base/xml/fn.xml"
     */
    private void xml(Path base, String filename, TaskDesc task) {
        confirmDir(base.resolve("xml"));
        String[][] fields = new String[task.fields.size()][2];
        for (int i = 0; i < task.fields.size(); i++) {
            TaskDesc.FieldDesc fd = task.fields.get(i);
            fields[i][0] = fd.dstName;
        }
        try (WriteToXml wtx = new WriteToXml(base.resolve("xml").resolve(filename + ".xml").toString())) {
            wtx.addInPutInfo(base.toString() + File.separator, filename, task.dstTableName);
            wtx.addOutPutInfo(base.toString() + File.separator);
            wtx.addFileds(fields);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

}
