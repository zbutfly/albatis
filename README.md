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
			List<Map<String, Object>> result = client.execute(sql, "");
		~~~
	
	1. 关闭客户端
		~~~java
			client.close();
		~~~
		
1. SQL功能支持
	1. 只支持查询语句，不支持插入、更新、修改、删除等操作
	
	1. 暂不支持函数
	
	1. 字段支持情况：
	    1. *, field_a, field_b[, field_c ...]
	    
	1. 表支持情况：
	    1. 能解析多表，但暂时只支持单表操作。
	    
	1. 过滤条件支持情况：
	    1. AND, OR, IS NULL, IS NOT NULL, >, >=, <, <=, =, <>, LIKE, BETWEEN, IN, NOT IN.
	    
	1. 排序支持情况
	    1. ORDER BY field_a ASC/DESC[, field_b ASC, DESC ...]
	    
	1. 查询数量支持情况
	    1. 支持 LIMIT 关键字(可选，不填则默认为 Integer.MAX_VALUE)
	    2. 支持 OFFSET 关键字(可选，不填则默认为 0)
	    3. OFFSET 需要在 LIMIT 之后出现 (如果有)