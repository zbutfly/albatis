# redis-io 
## 使用说明

#### 项目部署
1. 加入依赖

	* pom

			<dependency>
				<groupId>net.butfly.albatis</groupId>
				<artifactId>albatis-redis-io</artifactId>
				<version>1.5.3-SNAPSHOT</version>
			</dependency>
	
2. URISpec
	
		redis:list://<password>@<ip>:<port>?type=<string|byteArray>
	example:

		redis:list://b840fc02d524045429941cc15f59e41cb7be6c52@172.16.17.22:6379?type=string

3. 创建Connection对象

		RedisConnection rConn = Connection.DriverManager.connect(
				new URISpec("redis:list://b840fc02d524045429941cc15f59e41cb7be6c52@172.16.17.22:6379?type=string");
		
4. 创建RedisListInput对象

		RedisListInput input = new RedisListInput(String name, RedisConnection rConn, <String or byte[]> key);
	example:
		
		RedisListInput input = new RedisListInput("RedisListInput", rConn, "AFTER_VEHICLE");

5. 输出的数据结构

		Rmap(String keyName, Map<String, Object> value)
	
	注：如果要入kafka，需要配置key的值。

6. 注意事项

	* 现只支持内容为String或是byte[]类型，且数据格式为json的数据。