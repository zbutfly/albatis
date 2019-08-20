package net.butfly.albatis.odps;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TunnelException;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.OutputBase;
import net.butfly.albatis.io.Rmap;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

public class OdpsOutput extends OutputBase<Rmap> {

    private OdpsConnection conn;
    private TableSchema schema;
    private RecordWriter writer;

    protected OdpsOutput(String name, OdpsConnection connection) {
        super(name);
        this.conn = connection;
        this.schema = conn.uploadSession.getSchema();
        try {
            this.writer = conn.uploadSession.openBufferedWriter();
        } catch (TunnelException e) {
            e.printStackTrace();
        }
        closing(this::close);
        open();
    }

    @Override
    public void close() {
        try {
            writer.close();
            conn.uploadSession.commit();
        } catch (TunnelException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void enqsafe(Sdream<Rmap> items) {
        items.eachs(m -> {
            Record record = format(m);
            try {
                writer.write(record);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

    }

    private Record format(Map<String, Object> map) {
        Record record = conn.uploadSession.newRecord();
        for (int i = 0; i < schema.getColumns().size(); i++) {
            Column column = schema.getColumn(i);
            for (Map.Entry<String, Object> m : map.entrySet()) {
                Object obj = m.getValue();
                if (m.getKey().equals(column.getName())) {
                    switch (column.getType()) {
                        case INT:
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
