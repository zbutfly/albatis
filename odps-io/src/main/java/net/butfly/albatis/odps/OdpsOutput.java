package net.butfly.albatis.odps;

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static net.butfly.albacore.utils.Configs.get;

public class OdpsOutput extends OutputBase<Rmap> {
    private static final Logger logger = Logger.getLogger(OdpsOutput.class);
    private OdpsConnection conn;
    AtomicInteger count = new AtomicInteger();
    AtomicInteger totalcount = new AtomicInteger();
    long last = System.currentTimeMillis();

    static {
        Configs.of(System.getProperty("dpcu.conf", "dpcu.properties"), "dataggr.migrate");
    }

    public static final int ODPS_FLUSH_COUNT = Integer.parseInt(get("odps.flush.count", "10000"));
    public static final int ODPS_FLUSH_MINS = Integer.parseInt(get("odps.flush.minutes", "1"));
    ConcurrentHashMap<String,OdpsSession> odpsMap = new ConcurrentHashMap<>();

    protected OdpsOutput(String name, OdpsConnection connection) {
        super(name);
        this.conn = connection;
        closing(this::close);
        open();
    }

    private void upLoad(){
        ConcurrentHashMap<String,OdpsSession> odps = new ConcurrentHashMap<>();
        odps.putAll(odpsMap);
        odpsMap.clear();
        for (Map.Entry<String, OdpsSession> map : odps.entrySet()) {
            try {
                if (null != map.getValue().writer)map.getValue().writer.close();
                map.getValue().uploadSession.commit();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (TunnelException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() {
        upLoad();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
        }
    }

    @Override
    protected void enqsafe(Sdream<Rmap> items) {
            items.eachs(m -> {
                totalcount.incrementAndGet();
                if (null == conn.partition && !odpsMap.contains(m.table().name)) {
                    OdpsSession odpsSession = new OdpsSession(conn.tunnel, conn.project, m.table().name, null);
                    odpsMap.put(m.table().name, odpsSession);
                } else if (!odpsMap.containsKey(m.get(conn.partition).toString()) && m.containsKey(conn.partition)) {
                    conn.CreatPartition(m.get(conn.partition).toString());
                    PartitionSpec partitionSpec = new PartitionSpec();
                    partitionSpec.set(conn.partition, m.get(conn.partition).toString());
                    OdpsSession odpsSession = new OdpsSession(conn.tunnel, conn.project, conn.tableName, partitionSpec);
                    odpsMap.put(m.get(conn.partition).toString(), odpsSession);
               }
                OdpsSession odpsSession;
                if (null == conn.partition) {
                    odpsSession = odpsMap.get(m.table().name);
                } else {
                    odpsSession = odpsMap.get(m.get(conn.partition).toString());
                }
                Record record = format(m, odpsSession);
                try {
                    odpsSession.writer.write(record);
                    count.incrementAndGet();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                synchronized (this) {
                    if (count.intValue() > 0 && (count.intValue() % ODPS_FLUSH_COUNT == 0 || System.currentTimeMillis() - last > ODPS_FLUSH_MINS * 60000)) {
                        logger.info("insert records count:" + count.intValue());
                        logger.info("total count:" + totalcount.intValue());
                        count.set(0);
                        last = System.currentTimeMillis();
                        upLoad();
                    }
                }
            });
    }

    private Record format(Map<String, Object> map,OdpsSession odpsSession) {
        Record record = odpsSession.uploadSession.newRecord();
        for (int i = 0; i < odpsSession.schema.getColumns().size(); i++) {
            Column column = odpsSession.schema.getColumn(i);
            for (Map.Entry<String, Object> m : map.entrySet()) {
                Object obj = m.getValue();
                if (m.getKey().equals(column.getName())) {
                    switch (column.getType()) {
                        case INT:
                            record.setBigint(column.getName(), Long.valueOf(obj.toString()));
                            break;
                        case BIGINT:
                            record.setBigint(column.getName(), Long.valueOf(obj.toString()));
                            break;
                        case BOOLEAN:
                            record.setBoolean(column.getName(), Boolean.valueOf(obj.toString()));
                            break;
                        case DATETIME:
                            record.setDatetime(column.getName(), (Date) obj);
                            break;
                        case DOUBLE:
                            record.setDouble(column.getName(), Double.valueOf(obj.toString()));
                            break;
                        case VARCHAR:
                            record.setString(column.getName(), obj.toString());
                            break;
                        case STRING:
                            record.setString(column.getName(), obj.toString());
                            break;
                        default:
                            record.setString(column.getName(), obj.toString());
                            break;
                    }
                }
            }
        }
        return record;
    }

    private class OdpsSession{
        TableSchema schema;
        RecordWriter writer;
        TableTunnel.UploadSession uploadSession;

        OdpsSession(TableTunnel tunnel,String project,String tableName, PartitionSpec partition){
            try {
                if (null != partition) {
                    uploadSession = tunnel.createUploadSession(project, tableName, partition);
                } else {
                    uploadSession = tunnel.createUploadSession(project, tableName);
                }
                schema = uploadSession.getSchema();
                writer = uploadSession.openBufferedWriter();
            } catch (TunnelException e) {
                logger.error("get uploadSession fail: "+e);
            }
        }
    }

}
