# 统一查询
## 详细设计

###	项目结构

####	uniquery-core 
1.	说明
	统一搜索核心代码块。实现资源管理、SQL解析。

1.	模块
	1.	数据资源
		1.	数据资源管理类
			~~~java
				class ClientManager {
					
					// 资源创建
					public static Client getConnection(URISpec uriSpec) {...};
					// 资源池
					private void cacheConnection(URISpec uriSpec, Client client) {...};
					// 资源释放
					public void releaseConnection(Connection conn) {...};
				}
			~~~
		1.	数据资源客户端	
			~~~java
				class Client implements AutoCloseable {
					// 创建客户端
					public Client(URISpec uriSpec) {...}
					// 执行sql
					public <T> T execute(String sql, Object...params) {...}
					// 关闭客户端
					public void close() throws Exception {...}
				}
			~~~
	1.	SQL解析
		1.	SQL解析类
			~~~java
				class SqlExplainer {
					
					// 解析SQL
					public static SqlNode explain(String sql) {...};
				}
			~~~
		1.	适配器接口
			~~~java
				interface Adapter {
					// 根据uriSpec获取对应的适配器实现
					public static Adapter adapt(URISpec uriSpec) {...}
					
					// 查询组装
				    public <T> T queryAssemble(SqlNode sqlNode, Object...params);
				    
				    // 查询执行
				    public <T> T queryExecute(Connection connection, Object query);
				    
				    // 结果组装
				    public <T> T resultAssemble(Object result);
				}
			~~~
####	uniquery-solr 
1.	说明 
	solr搜索模块。实现创建资源连接、查询组装、查询执行、结果组装、关闭资源连接。
1.	模块 
	1.	适配器
		~~~java
			class SolrAdapter implements Adapter {
				
				// 查询组装
				public SolrParams queryAssemble(SqlNode sqlNode, Object...params) {...}
				
				// 查询执行
				public QueryResponse queryExecute(Connection connection, Object solrParams) {...}
				
				// 结果组装
				public <T> T resultAssemble(Object queryResponse) {...}
			}
		~~~

####	uniquery-es2
1.	说明 
	es2搜索模块。实现创建资源连接、查询组装、查询执行、结果组装、关闭资源连接。
1.	模块
####	uniquery-es5
1.	说明 
	es5搜索模块。实现创建资源连接、查询组装、查询执行、结果组装、关闭资源连接。
1.	模块
####	uniquery-auth
1.	说明 
	认证授权
1.	模块
####	uniquery-field-mask
1.	说明 
	返回结果的字段红/黑名单
1.	模块