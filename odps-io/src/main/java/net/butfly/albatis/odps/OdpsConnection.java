package net.butfly.albatis.odps;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.Connection;
import net.butfly.albatis.DataConnection;
import net.butfly.albatis.ddl.TableDesc;

import java.io.IOException;
import java.util.List;

public class OdpsConnection extends DataConnection<Connection> {
    private static final Logger logger = Logger.getLogger(OdpsConnection.class);

    private static String accessId ;
    private static String accessKey ;
    private static String odpsUrl ;
    //默认情况下，使用公网进行传输。如果需要通过内网进行数据传输，必须设置tunnelUrl变量。
    private static String tunnelUrl ;
    static String project ;
    String partition ;
    TableTunnel tunnel;
    String tableName;
    private Odps odps;

    public OdpsConnection(URISpec uri) throws IOException {
        super(uri, "odps:http");
        String uriSpec = uri.toString();
        odpsUrl = uriSpec.substring(uriSpec.indexOf(":")+1,uriSpec.indexOf("?"));
        accessId = uri.getParameter("accessId");
        accessKey = uri.getParameter("accessKey");
        project = uri.getParameter("project");
        tunnelUrl = uri.getParameter("tunnelUrl");
        partition = uri.getParameter("partition");
        init();
    }

    private void init(){
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
        return new OdpsOutput("OdpsOutput",this);
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

}
