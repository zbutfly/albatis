# 文件输入输出

## 支持

基于apache commons-vfs2实现。直接在`albatis-core`支持。

当前支持两种模式：

- 本地文件
- 主动模式ftp

### URI

- 本地（**vfs不支持相对路径，必须用绝对路径**）：
	- `vfs:///C:/temp/..../def.json`
	- `vfs:file:///C:/temp/..../`
	- `file:///C:/temp/a/b/c/default.json`
	- `file:///C:/temp/..../`
	- `file:///C:/temp/a/b/c/default.json`
- ftp（**vfs不支持绝对路径，只支持相对于当前用户home的相对路径**）
	- `vfs:ftp://127.0.0.1/test2/def.json`
	- `ftp://127.0.0.1/test2/`
	- `ftp://user:pwd@127.0.0.1/test2/`

### URI参数

`?px=prefix&sx=suffix&limit=5000`

- `limit`是文件rolling的上限，默认5000，定义值小于等于0则不rolling（同一文件输出）。
- `prefix`和`suffix`是文件rolling更名的前缀和后缀。日期字符串。会在rolling时间点用这两个参数生成新的文件名。
	- 可以用prefix实现目录hash：prefix=yyyy/MM/dd/。此时应注意定义suffix使文件名不重复。
- 若`limit`大于0（rolling）而`prefix`/`suffix`均未定义，默认`suffix=yyyyMMddhhmmssSSS`

### 注意

- 目录必须在最后加/，否则会被当做默认文件名。
- 各层子目录会自动创建。
- 默认文件名当记录没有目标表名（文件名）时作为文件名输出。
- 文件扩展名可以在schema里加，没有的话用默认文件名的扩展名。如果都没有，默认`.json`。
- 文件扩展名和文件数据格式无关，现在只支持json格式输出。可随意定义扩展名（比如`.cominfo`/`.hik`）。
- 文件默认append模式，如果失败会用overwrite模式。
- 文件换行使用java，也就是当前os，也就是在windows上运行就是CRLF，在UNIX上就是LF。编码统一UTF8。

## 问题：

### FTP协议

协议复杂，服务器兼容性差。

我换了n个ftp服务器才找到一个兼容APPE指令的。

**必须全面测试客户的服务器**。

### 运行时依赖

当前我在依赖里加了ftp库。要支持新的fs，需要加新库（比如：hdfs，或者webdev之类）。

### 扩展支持

vfs2实际上支持amazon s3（通过外挂附件）。

[![具体支持列表](vfs2-supports.jpg)](https://commons.apache.org/proper/commons-vfs/filesystems.html)

