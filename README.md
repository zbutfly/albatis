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
			String uri = "solr:http://cdh001:10180/solr";
			//String uri = "zk:solr://cdh001:12181,cdh002:12181,cdh003:12181";
			URISpec uriSpec = new URISpec(uri);
		~~~
		
	1. 创建统一搜索客户端
		~~~java
			Client client = new Client(uriSpec);
		~~~
		
	1. 执行sql并得到结果
		1. common query
			~~~java
				String sql = "select * from tdl_c1 where id=? and XB_FORMAT_s=?";
				Object[] params = {"593fb635c00282f4af41bdd0","男"}; //动态参数
				List<Map<String, Object>> result = client.execute(sql, params);			
			~~~
		
		2. facet 
			~~~java
				String sql = "select XB_FORMAT_s,MZ_FORMAT_s from tdl_c1 group by XB_FORMAT_s,MZ_FORMAT_s";
				Object[] params = {};
				List<Map<String, Object>> result = client.execute(sql, params);			
			~~~
			
		1. multi facet
			~~~java
				String sql = "select * from tdl_c1";
				String[] facets = {"XB_FORMAT_s", "MZ_FORMAT_s", "MZ_i"}; //facet字段 一个数组元素可包含多个facet字段,用  , 隔开
				Object[] params = {};
				List<List<Map<String, Object>>> result = conn.execute(sql, facets, params);	
			~~~
	
	1. 关闭客户端
		~~~java
			client.close();
		~~~
		
1. SQL功能支持
	1. 只支持查询语句，不支持插入、更新、修改、删除等操作
	
	2. 暂不支持函数
	
	3. 字段支持情况：
	    1. *, field_a, field_b[, field_c ...]
	    
	4. 表支持情况：
	    1. 能解析多表，但暂时只支持单表操作。
	    
	5. 过滤条件支持情况：
	    1. 不指定, AND, OR, IS NULL, IS NOT NULL, >, >=, <, <=, =, <>, LIKE, NOT LIKE, BETWEEN AND, NOT BETWEEN AND, IN, NOT IN.
	    
	6. 排序支持情况
	    1. ORDER BY field_a ASC/DESC[, field_b ASC, DESC ...]
	    
	7. 查询数量支持情况
	    1. 支持 LIMIT 关键字(可选，不填则默认为 Integer.MAX_VALUE)
	    2. 支持 OFFSET 关键字(可选，不填则默认为 0)
	    3. OFFSET 需要在 LIMIT 之后出现 (如果有)
	
	8. 动态参数支持情况
	    1. 字段列表、查询条件中支持动态参数(用 ? 占位), 在explain时需要把参数传入
	
	9. cache功能
	    1. 以HashMap方式简单支持 