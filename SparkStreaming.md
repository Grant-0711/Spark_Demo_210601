# 一、相关概念

## 1.几个概念

流式数据：   源源不断产生的数据 

离线数据： 离线（不在线）过去的数据，不是当下的数据！

实时数据： 当前正在产生的数据！

离线计算： 指时效性低的计算，计算需要一段时间才能完成，不能立刻完成！

实时计算： 在规定的时效范围内完成运算！

离线计算和实时计算的区别在于时效性！时效性通常需要根据业务进行自定义！不同的公司对于不同的应用场景，有不同的定义！

不管是离线计算还是实时计算，都可以计算流式数据！ 通常实时计算的时效性强，更适合处理流式数据！

离线计算，时效性低，更适合处理批量离线数据！



## 2.SparkStreaming的处理方式

SparkStreaming 适合准（接近）实时，不是真正的实时！

SparkStreaming  基于SparkCore

Spark Core 用来 批处理计算！

 SparkStreaming  需要将数据持久化，离散化，批处理！

持久化： 根据采集周期，将一个采集周期的数据，进行持久化存储

离散化： 讲流式数据，根据采集周期，离散为批数据

批处理： 底层使用RDD，进行处理！ 每1批数据，都会启动一个Job来进行计算！

Spark Streaming   微批次处理！



真正的实时： 来一条数据，就计算处理一次，可以在规定的失效范围内计算完成！



SparkStreaming  不是真正的实时计算框架，但是可以用于实时计算？

​		SparkStreaming  可以满足 部分中小企业的实时业务场景需求！



​		

## 3.编程模型

SparkCore:    SparkContext (编程环境)   RDD，BroadCast,Accumulator(编程模型)

SparkSQL:    SparkSession((编程环境)  DataFrame,DataSet(编程模型)

SparkStreaming:   DStream(编程模型)



# 二、WordCount

## 1.工具

netcat

```
sudo yum -y install nc
```

使用：

```
nc -l hadoop103 3333    //监听一次请求
nc -kl hadoop103 3333    //一直监听
```

