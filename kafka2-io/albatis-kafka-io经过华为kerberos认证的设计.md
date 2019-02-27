# albatis-kafka-io经过华为的kerberos认证的设计

为了调用华为集群的kafka，需要通过华为的kerberos认证。

## 调查准备

根据华为提供的文档，请看
[华为二次开发环境准备](https://forum.huawei.com/enterprise/zh/thread-471209.html)

我们需要传递如下信息：

- krb5.conf文件的路径（由集群部署方提供），需要加入到java系统参数中。
- user.keytab文件的路径（由集群部署方提供）
- kafka principal（由集群部署方提供），跟user.keytab文件路径一起通过一定处理后生成kafka\_server\_jaas.conf文件，并把该文件路径加入到java系统参数中。
- zk principal（由集群部署方提供），需要加入到java系统参数中。
- 额外的配置（由集群部署方提供），这是kafka认证的一些参数，需要加入到kafka的连接配置中去，分为consumer和producer。

## 设计

### 配置文件
需要有五个配置文件存放以上信息，并将五个文件放入指定路径（将此指定路径配置入程序启动的classpath中）。

1. krb5File.conf

		存放krb5.conf文件路径。

2. principalsFile.conf
	
		存放kafka principal和zk principal，第一行为kafka principal，第二行为zk principal

3. jaasFile.conf
		
		存放user.keytab文件的路径

4. consumerFile.conf

		存放consumer配置文件的路径

5. producerFile.conf

		存放producer配置文件的路径

### 程序实现
在kafka连接url中加入参数。

1. kerberosHuawei=true 控制是否打开华为的kerberos认证，默认为false
2. krb5File=krb5File.conf krb5File的文件名，系统会去classpath中找该文件，默认为null
3. jaasFile=jaasFile.conf user.keytab的文件名，系统会去classpath中找该文件，默认为null
4. principalsFile=principalsFile.conf kafka和zk的principals，系统会去classpath中找该文件，默认为null
5. consumerFile=consumerFile.conf，consumerFile的文件名，系统会去classpath中找该文件，默认为null
6. producerFile=producerFile.conf，producerFile的文件名，系统会去classpath中找该文件，默认为null

	- 1~4将在KafkaConfigBase中实现
	- 5在Kafka2InputConfig中实现
	- 6在Kafka2OutputConfig中实现