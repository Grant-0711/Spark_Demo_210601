# 1 数仓开发技术

数仓： 需要使用专门的数据仓库软件开发！

使用Hive或者SparkSql



1.0：  Hive   引擎  MR

2.0 ： Hive  引擎 Tez

3.0 :   Hive  引擎 Spark



## 1.1 Hive  on  (MR/Tez/Spark)

**使用流程：**

打开hive-cli/beeline-hiveserver2-------> HQL  --------> Hive的语法解析器解析----->根据配置的 (MR/Tez/Spark)引擎生成

基于 (MR/Tez/Spark)的执行的Job



本质特色： 输入，输出，计算程序都由Hive完成！ Hive生成的Spark Job是模版代码，是固定的！



## 1.2 SparkSql 

计算的数据由Hive管理



SparkSql 可以对接多种数据源



Spark on  (Hive/MySql/Redis)

Spark on Hive 读取的是hive中的数据

Spark on MySql 读取是MySql中的数据

 Spark on Redis 读取的是Redis中的数据



**使用流程：**

打开spark-shell / spark-sql / spark-submit xxx.jar / beeline - start-thriftserver.sh ---------> HQL / 代码(DSL)  ---------> Spark的语法解析器解析----> DAG ----> 生成Job

本质特色： 输入，输出，由Hive完成！计算程序都由Spark负责完成！







------

## 1.3 Spark on Hive 和 Hive on Spark 区别

https://www.processon.com/diagraming/60d36d261efad461e44c3bf1



相同：数据都由Hive管理！ 生成的Job都是Spark代码编写的Job!



区别： 

①使用的工具不同

​	Spark on Hive，打开spark提供的工具

​	Hive on Spark ，使用Hive

②生成的Job，执行效率不同

​	Hive on Spark ：  HQL  ------> 由Hive的语法解析器解析 ------>翻译为Spark的Job

​	Spark on Hive：  HQL   ------> 由Spark的语法解析器解析 ------>翻译为Spark的Job

​	

​	spark官方自称，使用spark sql后优化的任务比Hive On Spark快10倍。



③稳健性

​	Spark on Hive：  框架升级和bug修复，由spark社区维护，社区活跃，框架稳健

​	Hive on Spark ：  框架升级和bug修复，由Hive维护， 社区不活跃，官方更新意愿不强。

​	官方只测试了若干特定版本，其他版本不负责！

| Hive Version | Spark Version |
| :----------- | :------------ |
| master       | 2.3.0         |
| 3.0.x        | 2.3.0         |
| 2.3.x        | 2.0.0         |
| 2.2.x        | 1.6.0         |
| 2.1.x        | 1.6.0         |
| 2.0.x        | 1.5.0         |
| 1.2.x        | 1.3.1         |
| 1.1.x        | 1.2.0         |

④灵活性

​	Hive on Spark ：  HQL

​	Spark on Hive：  DSL/ HQL/ DF、DS 转 RDD

⑤生态兼容

​	Hive：  生态和兼容性好

​	Spark ：  对比Hive，没有那么好



## 1.4 Hive on Spark原理

https://www.processon.com/diagraming/60d36a7c5653bb049a4427ec

①HQL语句会在Hive中翻译为Spark-Job

②上述过程需要配置Spark环境变量以及在Hive机器上安装Spark3.0

③Spark-Job运行模式为YARN，所以需要在HDFS上上传纯净的sparkJar包