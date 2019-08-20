package net.butfly.albatis.odps;

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
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
    private static String tunnelUrl ;
    private static String project ;
//    private static String partition ;

    private Odps odps;
    TableTunnel.UploadSession uploadSession;

    public OdpsConnection(URISpec uri) throws IOException {
        super(uri, "odps:http");
        String uriSpec = uri.toString();
        odpsUrl = uriSpec.substring(uriSpec.indexOf(":")+1,uriSpec.indexOf("?"));
        accessId = uri.getParameter("accessId");
        accessKey = uri.getParameter("accessKey");
        project = uri.getParameter("project");
        tunnelUrl = uri.getParameter("tunnelUrl");
        init();
    }

    private void init(){
        Account account = new AliyunAccount(accessId, accessKey);
        odps = new Odps(account);
        odps.setEndpoint(odpsUrl);
        odps.setDefaultProject(project);
    }

    private void getUploadSession(String table){
        try {
            TableTunnel tunnel = new TableTunnel(odps);
            tunnel.setEndpoint(tunnelUrl);
//            PartitionSpec partitionSpec = new PartitionSpec(partition);
//            uploadSession = tunnel.createUploadSession(project, table, partitionSpec);
            uploadSession = tunnel.createUploadSession(project, table);
            logger.info("Session Status is : " + uploadSession.getStatus().toString());
        } catch (TunnelException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public OdpsOutput outputRaw(TableDesc... table) throws IOException {
        getUploadSession(table[0].qualifier.name);
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
