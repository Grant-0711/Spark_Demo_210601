# 一、简介

## 1.SparkSql和HiveOnSpark



SparkSql: 是spark中支持sql处理结构化数据的一个模块。

​				

​			①	既支持sql处理数据 ，还支持使用代码(RDD)，更灵活

​		    ②    对接多种数据源，数据可以在Mysql，可以在redis，可以在HBase，可以在ES，可以在Hive中，数据源支持完善

​			③  SparkSql 无缝支持 HQL。

​					如果使用的是SparkSql，处理的数据是Hive管理起来的数据，也称为Spark On Hive

​                   如果使用的是SparkSql，处理的数据是Mysql管理起来的数据，也称为Spark On Mysql







Hive：   ① HQL(类sql)，编程方式单一

​			   ② hive处理的数据默认都是存储在HDFS





------------------

​	同：    计算的数据都是由Hive进行管理，使用Hive建表，将数据导入到表中

​	不同：

SparkOnHive:  ①写HQL处理，也可以写代码处理

​							②只需要按照Spark，使用的是 SPARK_HOME/bin/spark-sql  | spark-shell |  spark-submit

​							③程序运行的效率不同

​									HQL -----------> Spark的 SQL解析器解析--------->Spark的SQL优化器优化--------->Spark App

​											sparksql更优，官方声称，是HiveOnSpark的十倍

​		



HiveOnSpark:  ①只能写HQL

​							②本质上使用的是Hive, 安装Hive，使用HIVE_HOME/bin/hive（cli），hiveserver2

​						   ③	HQL -----------> Hive的 SQL解析器解析--------->Hive的SQL优化器优化--------->Spark App



## 2.DF和DS



RDD： RDD是spark core最基本的编程模型



DataFrame=  RDD(数据)  +  schame(元数据)   1.3推出

​							弱类型。 



DataSet : 1.6推出。 将DataFrame代码重写，将DataFrame作为 DataSet的特例

​									Dataframe=  DataSet[Row]

​									强类型。



Row: 一行数据



