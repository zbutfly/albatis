# 项目介绍
        项目主要功能用于传输对接烽火数据，经dpc页面配置字段信息，输入源，输出文件路径等，抽取相应表数据生成烽火要求的ZIP文件，
    zip传输ftp验证文件，字段有值数记录文件，并通过ftp发送到相应路径下，本地也会存放一份数据。数据存放在配置路径下的/zip/表名
    文件夹下。dpc页面配置目每个任务的标表名需要和对应的任务名相同。
        可能会使用表达式urlField2Base64 处理小图url字段，新增字段存放表达式转换成base64的字符串
    说明：将传入存着url的字段，发送get请求，拿到byte[]，转成base64后返回
    返回：String
    实例：urlField2Base64(PIC_URL)
        使用表达式replace 处理数据中的\n\t特殊字符
    说明：将传入原字段,待替换字符串,替换字符串，调用java replace替换后返回
    返回：String
    实例:replace("李\n是\n多\t少","[\n\t]+","")
    

# 配置说明
    dpc配置mysql库信息
    dataggr.migrate.config=jdbc:mysql://172.16.17.41:3306/hk_dpc_test?user=hk_dpc_test&password=Hk_dpc@test123!&characterEncoding=UTF-8&useSSL=false
    
    //本地存放数据文件路径
    dataggr.migrate.bcp.path.base=./

    ftp对应的ip,端口,用户,密码,保存文件路径
    dataggr.migrate.ftp=ftp-user1:Hik123@172.16.17.47:21/

    每批打包数据量(默认10000)
    dataggr.migrate.bcp.flush.count=10000
    更新数据少时打包间隔时间(分钟，默认30)
    dataggr.migrate.bcp.flush.idle.minutes=5
    bcp线程池大小，默认50
    dataggr.migrate.bcp.parallelism=50
    httpclent连接数
    dataggr.migrate.http.parallelism=100
    httpclent超时时间，默认30000
    dataggr.migrate.http.timeout.ms=300000
    是否删除临时文件，默认true
    dataggr.migrate.bcp.tempfields.clean=true
    
#页面配置说明
    input url连接串：bcp:///C:\Users\zhuqh\Desktop\test
    url后半段路径为bcp文件的本地存放路径

    output url连接串：bcp:///
    输出表名需要配置成当前任务名
    
#启动
    ./run.sh <dpc任务名>

