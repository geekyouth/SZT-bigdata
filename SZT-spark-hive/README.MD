# spark on hive 踩坑记录

## lzo 压缩问题

如果使用了 LZO ，开发主机必须和远程环境保持一致的 lzo 环境，否则无法解码：Compression codec com.hadoop.compression.lzo.LzopCodec not found，（hadoop-lzo-0.4.15-cdh6.2.1.jar），win10 适用的 lzo 本地库：
hadoop-lzo-0.4.21-ok | scala windows本地配置lzo读取所需相关组件_运维_coolerzZ的博客-CSDN博客
https://blog.csdn.net/coolerzz/article/details/103952188

客户端也可以禁用 LZO 压缩配置：hive 客户端配置文件全局搜索”compress“，关闭所有压缩选项；
java.lang.IllegalArgumentException: Compression codec com.hadoop.compression.lzo.LzoCodec not found. - 小象问答-hadoop,spark,storm,R,hive,hbase,redis,大数据问答社区
		https://wenda.chinahadoop.cn/question/2983

Cannot run program "/etc/hadoop/conf.cloudera.yarn/topology.py" (in directory "D:\，
Win7_64位MyEclipse2015以yarn-client提交spark到CDH5.10.0任务报错及解决方法 - 耿廑 - 博客园|  https://www.cnblogs.com/yesecangqiong/p/6567805.html，解决：core-site.xml，找到配置net.topology.script.file.name，将其value注释掉；

InvalidClassException: org.apache.spark.sql.catalyst.expressions.AttributeReference; local class incompatible: stream classdesc serialVersionUID =
yarn 远程提交，版本不兼容问题：无解，不推荐
