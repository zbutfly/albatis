# albatis-jdbc-io经过华为的ssl认证的设计

为了调用华为集群的libra，需要通过华为的ssl认证。

## 参考资料

根据华为提供的文档，请看
[华为二次开发环境准备](https://forum.huawei.com/enterprise/zh/thread-471209.html)

服务端配置
当开启SSL模式后，必须提供根证书、服务器证书和私钥。LibrA预置了这些证书，证
书文件被命名为cacert.pem(根证书)、server.crt（服务端证书）和server.key（服务端私
钥），并放置在了数据目录/gaussdb/data/data_cn下。当然，可以使用配置参数
ssl_ca_file、ssl_cert_file和ssl_key_file修改它们的名称和存放位置。
在Unix系统上，server.crt、server.key的权限设置必须禁止任何外部或组的访问，如果使
用用户自己的证书，请执行如下命令实现这一点。如果使用LibrA的内置证书，LibrA
已经提前设置了权限，不需要再重复设置。
chmod 0600 server.key
配置步骤（假设用户的证书文件放在数据目录下，且采用默认文件名）：
步骤1 以操作系统用户omm登录LibrA集群任意一台主机。执行source ${BIGDATA_HOME}/
mppdb/.mppdbgs_profile命令启动环境变量。
步骤2 开启SSL认证模式。
gs_guc set -Z coordinator -D ${BIGDATA_DATA_HOME}/mppdb/data1/coordinator -c "ssl=on"
步骤3 配置客户端接入认证参数。
gs_guc reload -Z coordinator -D ${BIGDATA_DATA_HOME}/mppdb/data1/coordinator -h "hostssl
all all 127.0.0.1/32 cert"
表示允许127.0.0.1/32网段的客户端以ssl认证方式连接到LibrA服务器。
步骤4 配置SSL认证相关的数字证书参数。
各命令后所附为设置成功的回显。
gs_guc set -Z coordinator -D ${BIGDATA_DATA_HOME}/mppdb/data1/coordinator -c
"ssl_cert_file='server.crt'"
gs_guc set: ssl_cert_file='server.crt'
gs_guc set -Z coordinator -D ${BIGDATA_DATA_HOME}/mppdb/data1/coordinator -c
"ssl_key_file='server.key'"
gs_guc set: ssl_key_file='server.key'
gs_guc set -Z coordinator -D ${BIGDATA_DATA_HOME}/mppdb/data1/coordinator -c
"ssl_ca_file='cacert.pem'"
gs_guc set: ssl_ca_file='cacert.pem'
步骤5 重启数据库。
gs_om -t stop && gs_om -t start

客户端配置
不同于基于gsql或其他基于libpq的程序，JDBC默认支持服务证书确认，如果用户使用
一个由认证中心(CA，全球CA或区域CA)签发的证书，则java应用程序不需要做什么，
因为java拥有大部分认证中心(CA，全球CA或区域CA)签发的证书的拷贝。如果用户使
用的是自签的证书，则需要配置客户端程序，使其可用，此过程依赖于openssl工具以
及java自带的keytool工具，配置步骤如下：
步骤1 将根证书导入到trustStore中。
openssl x509 -in cacert.pem -out cacert.crt.der -outform der
生成中间文件cacert.crt.der。
keytool -keystore mytruststore -alias cacert -import -file cacert.crt.der
请用户根据提示信息输入口令，此口令为truststorepassword，例如Gauss@123。生成
mytruststore。
l cacert.pem为根证书。
l cacert.crt.der为中间文件。
l mytruststore为生成的密钥库名称，此名称以及别名用户可以根据需要进行修改。
步骤2 将客户端证书和私钥导入到keyStore中。
openssl pkcs12 -export -out client.pkcs12 -in client.crt -inkey client.key
请用户根据提示信息输入clientkey，默认是Gauss@MppDB，export password，例如
Gauss#123。生成client.pkcs12。
keytool -importkeystore -deststorepass Bigdata@123 -destkeystore client.jks -srckeystore
client.pkcs12 -srcstorepass Gauss#123 -srcstoretype PKCS12 -alias 1 -destkeypass Bigdata@123
此处deststorepass与destkeypass需保持一致，srcstorepass需与上条命令中的export
password保持一致。生成client.jks。

### 配置文件

先通过上述的客户端配置得到 mytruststore 和 client.jks 以及对应的验证密码，需要配置到ssl.properties配置文件中。
如果不配置，没有ssl.properties配置文件，则不走ssl认证。
配置文件,约定的文件名：

 ssl.properties 

	javax.net.ssl.trustStore=mytruststore
    javax.net.ssl.keyStore=client.jks
    javax.net.ssl.trustStorePassword=Gauss@123   //trustStore验证密码
    javax.net.ssl.keyStorePassword=Bigdata@123   //keyStore验证密码

建议mytruststore和client.jks指定到文件绝对路径


### URI参数配置
如果需要ssl认证，要在url加上ssl=true
例如：jdbc:postgresql:libra://172.30.10.31:15432/cigpdb?user=cigpdb&password=cigpdb&ssl=true

