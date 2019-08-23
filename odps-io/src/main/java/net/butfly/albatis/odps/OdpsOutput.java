package net.butfly.albatis.odps;

import com.aliyun.odps.Column;
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
import java.util.concurrent.atomic.AtomicInteger;

import static net.butfly.albacore.utils.Configs.get;

public class OdpsOutput extends OutputBase<Rmap> {
    private static final Logger logger = Logger.getLogger(OdpsOutput.class);
    private OdpsConnection conn;
    private TableSchema schema;
    private RecordWriter writer;
    private TableTunnel.UploadSession uploadSession;
    AtomicInteger count = new AtomicInteger();
    static {
        Configs.of(System.getProperty("dpcu.conf", "dpcu.properties"), "dataggr.migrate");
    }
    public static final int ODPS_FLUSH_COUNT = Integer.parseInt(get("odps.flush.count", "1000"));

    protected OdpsOutput(String name, OdpsConnection connection) {
        super(name);
        this.conn = connection;
        try {
            this.uploadSession = conn.tunnel.createUploadSession(conn.project, conn.tableName);
            this.schema = uploadSession.getSchema();
            this.writer = uploadSession.openBufferedWriter();
        } catch (TunnelException e) {
            e.printStackTrace();
        }
        closing(this::close);
        open();
    }

    @Override
    public void close() {
        try {
            if(null!=writer)writer.close();
            this.uploadSession.commit();
        } catch (TunnelException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void enqsafe(Sdream<Rmap> items) {
        items.eachs(m -> {
            if(count.intValue()%ODPS_FLUSH_COUNT==0){
                try {
                    if(null!=writer)writer.close();
                    this.uploadSession.commit();
                logger.trace("Session Status is : " + uploadSession.getStatus().toString());
                    uploadSession = conn.tunnel.createUploadSession(conn.project, conn.tableName);
                    this.schema = uploadSession.getSchema();
                    this.writer = uploadSession.openBufferedWriter();
                logger.trace("Session2 Status is : " + uploadSession.getStatus().toString());
                } catch (TunnelException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            Record record = format(m);
            try {
                writer.write(record);
                count.incrementAndGet();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private Record format(Map<String, Object> map) {
        Record record = this.uploadSession.newRecord();
        for (int i = 0; i < schema.getColumns().size(); i++) {
            Column column = schema.getColumn(i);
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

}
