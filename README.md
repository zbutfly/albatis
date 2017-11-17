# 统一查询
## 使用说明

#### 本地调用 （以solr为例）
1. mvn 依赖
	~~~xml
		<dependency>
			<groupId>com.hzcominfo.dataggr.uniquery</groupId>
			<artifactId>uniquery-solr</artifactId>
			<version>0.0.1-SNAPSHOT</version>
		</dependency>
	~~~

1. 本地调用代码实例
	1. 根据url串创建URISpec实例
		~~~java
			String uri = "solr:http://172.16.17.11:10180/solr/tdl_c1";
			URISpec uriSpec = new URISpec(uri);
		~~~
		
	1. 创建统一搜索客户端
		~~~java
			Client client = new Client(uriSpec);
		~~~
		
	1. 执行sql并得到结果
		~~~java
			String sql = "select * from tdl_c1 where id='593fb635c00282f4af41bdd0' and XB_FORMAT_s='男'";
			List<Map<String, Object>> result = conn.execute(sql, "");
		~~~
	
	1. 关闭客户端
		~~~java
			client.close();
		~~~