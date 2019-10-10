package net.butfly.albatis.odps;

import com.aliyun.odps.*;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.Connection;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.Qualifier;
import net.butfly.albatis.ddl.TableDesc;

import java.io.IOException;
import java.util.List;

import static net.butfly.albatis.ddl.vals.ValType.Flags.*;

public class OdpsConnection extends DataConnection<Connection> {
    private static final Logger logger = Logger.getLogger(OdpsConnection.class);

    private static String accessId;
    private static String accessKey;
    private static String odpsUrl;
    //默认情况下，使用公网进行传输。如果需要通过内网进行数据传输，必须设置tunnelUrl变量。
    private static String tunnelUrl;
    static String project;
    String partition;
    TableTunnel tunnel;
    String tableName;
    private Odps odps;

    public OdpsConnection(URISpec uri) throws IOException {
        super(uri, "odps:http");
        String uriSpec = uri.toString();
        odpsUrl = uriSpec.substring(uriSpec.indexOf(":") + 1, uriSpec.indexOf("?"));
        accessId = uri.getParameter("accessId");
        accessKey = uri.getParameter("accessKey");
        project = uri.getParameter("project");
        tunnelUrl = uri.getParameter("tunnelUrl");
        partition = uri.getParameter("partition");
        init();
    }

    private void init() {
        Account account = new AliyunAccount(accessId, accessKey);
        odps = new Odps(account);
        odps.setEndpoint(odpsUrl);
        odps.setDefaultProject(project);
        tunnel = new TableTunnel(odps);
        tunnel.setEndpoint(tunnelUrl);
    }


    @Override
    public OdpsOutput outputRaw(TableDesc... table) throws IOException {
        tableName = table[0].qualifier.name;
        return new OdpsOutput("OdpsOutput", this);
    }

    public static class Driver implements net.butfly.albatis.Connection.Driver<OdpsConnection> {
        static {
            DriverManager.register(new Driver());
        }

        @Override
        public OdpsConnection connect(URISpec uriSpec) throws IOException {
            return new OdpsConnection(uriSpec);
        }

        @Override
        public List<String> schemas() {
            return Colls.list("odps:http");
        }
    }

    public void CreatPartition(String partitionValue) {
        PartitionSpec partitionSpec = new PartitionSpec();
        partitionSpec.set(partition, partitionValue);
        Table table = odps.tables().get(tableName);
        try {
            boolean a = table.hasPartition(partitionSpec);
            if (!a) {
                table.createPartition(partitionSpec);
            }
        } catch (OdpsException e) {
            logger.error("creat odps partition error: " + e);
        }
    }

    @Override
    public void construct(Qualifier qualifier, FieldDesc... fields) {
        Tables tables = odps.tables();
        try {
            boolean a = tables.exists(qualifier.name);
            if (!a) {
                TableSchema tableSchema = new TableSchema();
                Column col;
                for (FieldDesc fieldDesc : fields) {
                    if (null != partition && fieldDesc.name.equals(partition)) {
                        col = new Column(partition, getType(fieldDesc));
                        tableSchema.addPartitionColumn(col);
                    } else {
                        col = new Column(fieldDesc.name, getType(fieldDesc));
                        tableSchema.addColumn(col);
                    }
                }
                tables.create(qualifier.name, tableSchema);
            }
        } catch (OdpsException e) {
            logger.error("creat odps table error: " + e);
        }
    }

    private OdpsType getType(FieldDesc field) {
        OdpsType type;
        switch (field.type.flag) {
            case INT:
            case SHORT:
            case BINARY:
            case LONG:
                type = OdpsType.BIGINT;
                break;
            case FLOAT:
                type = OdpsType.FLOAT;
                break;
            case DOUBLE:
                type = OdpsType.DOUBLE;
                break;
            case BYTE:
            case BOOL:
                type = OdpsType.BOOLEAN;
                break;
            case STR:
            case CHAR:
            case UNKNOWN:
                type = OdpsType.STRING;
                break;
            case DATE:
                type = OdpsType.DATE;
                break;
            case TIMESTAMP:
                type = OdpsType.TIMESTAMP;
                break;
            default:
                type = OdpsType.STRING;
                break;
        }
        return type;
    }

}
