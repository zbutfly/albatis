package net.butfly.albatis.bcp;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.Connection;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.pump.Pump;
import java.io.IOException;
import java.net.URLEncoder;


public class FtpTest {

    public static void main(String[] args){
        //UnZip.unZip("D:\\项目\\烽火\\999-330000-1553600617-00677-hhh-0.zip", Paths.get("C:\\Users\\zhuqh\\Desktop\\net.butfly.albatis.bcp.FtpTest"));

        /*URISpec uriSpec = new URISpec("bcp:///data/bcp-net.butfly.albatis.bcp.FtpTest");
        String path = uriSpec.getPath();
        System.out.println(path);*/

        /*List<String> names = FileUtil.getFileNames("zip","hhh","C:\\Users\\zhuqh\\Desktop\\net.butfly.albatis.bcp.FtpTest");
        System.out.println(names.toString());*/

        //Path path = Paths.get("C:\\Users\\zhuqh\\Desktop\\net.butfly.albatis.bcp.FtpTest");
        //FileUtil.confirmDir(path.resolve("bcp").resolve("aaa").resolve("bbb"));

        //List<String> fields = readXml("D:\\项目\\烽火\\999-330000-1557995952-00543-mq_HIK_SNAP_IMAGE_ST-9979-0\\GAB_ZIP_INDEX.xml");

//        String zipName = "C:\\Users\\zhuqh\\Desktop\\net.butfly.albatis.bcp.FtpTest\\999-330000-1557995952-00543-mq_HIK_SNAP_IMAGE_ST-9979-0.zip";
//        Path path = Paths.get("C:\\Users\\zhuqh\\Desktop\\net.butfly.albatis.bcp.FtpTest\\999-330000-1557995952-00543-mq_HIK_SNAP_IMAGE_ST-9979-0");
//        UnZip.unZip(zipName,path);

        try (Connection oconn = Connection.connect(new URISpec("mongodb://unify:unify2233@10.33.41.52:30012/unify"));
             Output<Rmap> out = oconn.output("bcp_test");
             Connection iconn = Connection.connect(new URISpec("bcp:ftp://ftpuser001:ftpuser001@10.33.41.56:21/bcp"));
             Input<Rmap> in0 = iconn.input("mq_HIK_SNAP_IMAGE_ST");
             Pump<Rmap> p =in0.then(m ->m.keyField("UUID")).pump(2, out);) {
            p.open();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
