# 统一查询
## 详细设计

### 项目结构

#### uniquery-core 
1. 说明
	统一搜索核心代码块。

1. 模块
	1. 数据资源
		1. 客户端管理类
			~~~java
				class ClientManager {
					// 资源创建
					public static Client getClient(URISpec uriSpec) {...};
					// 资源管理
					private void cacheClient(URISpec uriSpec, Client client) {...};
					// 资源释放
					protected void releaseClient(Client client) {...};
				}
			~~~	
		1. 客户端接口	
			~~~java
				interface Client {
					// 创建client
					public Client getInstance(URISpec uriSpec);
					// 查询
					List<Map<String, Object>> select(String sql);
					// 查询一条
					Map<String, Object> selectOne(String sql);
					// 统计
					Long count(String sql);
					// 关闭client
					public void close();
				}
			~~~	
	1. SQL解析
		1. 适配器接口
			~~~java
				interface Adapter {
					// 根据sql得到组装类
					Translation translate(String sql);
				}
			~~~	
		1. 组装类 （翻译类）
			~~~java
				class Translation {...} 
			~~~	
		
#### uniquery-solr 
1. 说明 
	实现solr的适配解析和查询返回。
1. 模块 
	1. 客户端
		~~~java
			class SolrClient implements Client {
				// 创建client
				public Client getInstance(URISpec uriSpec) {...};
				// 查询
				List<Map<String, Object>> select(String sql) {...};
				// 查询一条
				Map<String, Object> selectOne(String sql) {...};
				// 统计
				Long count(String sql) {...};
				// 关闭client
				public void close() {...};
			}
		~~~	
	1. 适配器
		~~~java
			class SolrAdapter implements Adapter {
				Translation translate(String sql) {...};
			}
		~~~	

#### uniquery-es2
1. 说明 
	实现es2的适配解析和查询返回。
1. 模块
#### uniquery-es5
1. 说明 
	实现es5的适配解析和查询返回。
1. 模块
#### uniquery-auth
1. 说明 
	认证授权
1. 模块
#### uniquery-field-mask
1. 说明 
	返回结果的字段红/黑名单
1. 模块