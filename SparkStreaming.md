# 一.相关概念

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



Spark Streaming  不是真正的实时计算框架，但是可以用于实时计算？

Spark Streaming  可以满足 部分中小企业的实时业务场景需求！



​		

## 3.编程模型

Spark Core:    Spark Context (编程环境)   RDD，Broadcast,Accumulator(编程模型)

Spark Sql:    Spark Session((编程环境)  Data Frame,Data Set(编程模型)

Spark Streaming:   DStream(编程模型)



# 二.WordCount

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

## 2.流程

1.初始化Spark配置信息---->2.接受net cat端口发来的信息---->3.处理该批次单词统计的逻辑---->4.启动任务并且阻塞

## 3.代码

添加依赖：

```xml
<dependencies>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-streaming_2.12</artifactId>
        <version>3.0.0</version>
    </dependency>

    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-core_2.12</artifactId>
        <version>3.0.0</version>
    </dependency>
</dependencies>
```

编写代码：

```java
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount {

    def main(args: Array[String]): Unit = {

        //1.初始化Spark配置信息
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")

        //2.初始化SparkStreamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        //3.通过监控端口创建DStream，读进来的数据为一行行
        val lineDStream = ssc.socketTextStream("hadoop102", 9999)

        //3.1 将每一行数据做切分，形成一个个单词
        val wordDStream = lineDStream.flatMap(_.split(" "))

        //3.2 将单词映射成元组（word,1）
        val wordToOneDStream = wordDStream.map((_, 1))

        //3.3 将相同的单词次数做统计
        val wordToSumDStream = wordToOneDStream.reduceByKey(_+_)

        //3.4 打印
        wordToSumDStream.print()

        //4 启动SparkStreamingContext
        ssc.start()
        // 将主线程阻塞，主线程不退出
        ssc.awaitTermination()
```

更改日志打印级别

将log4j.properties文件添加到resources里面，就能更改打印日志的级别为error

```xml
log4j.rootLogger=error, stdout,R
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss,SSS}  %5p --- [%50t]  %-80c(line:%5L)  :  %m%n

log4j.appender.R=org.apache.log4j.RollingFileAppender
log4j.appender.R.File=../log/agent.log
log4j.appender.R.MaxFileSize=1024KB
log4j.appender.R.MaxBackupIndex=1

log4j.appender.R.layout=org.apache.log4j.PatternLayout
log4j.appender.R.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss,SSS}  %5p --- [%50t]  %-80c(line:%6L)  :  %m%n
```

## 4.WordCount解析

DStream是Spark Streaming的基础抽象，代表持续性的数据流和经过各种Spark算子操作后的结果数据流。

在内部实现上，每一批次的数据封装成一个RDD，一系列连续的RDD组成了DStream。对这些RDD的转换是由Spark引擎来计算。

说明：DStream中批次与批次之间计算相互独立。如果批次设置时间小于计算时间会出现计算任务叠加情况，需要多分配资源。通常情况，批次设置时间要大于计算时间。

# 三.DStream创建

## 3.1 RDD队列

### 3.1.1 用法及说明

测试方法：
（1）使用ssc.queueStream（queueOfRDDs）来创建DStream
（2）将每一个推送到这个队列中的RDD，都会作为一个DStream处理。

### 3.1.2 案例实操

需求：循环创建几个RDD，将RDD放入队列。通过Spark Streaming创建DStream，计算WordCount

任务流程：

1.初始化spark配置信息---->2.接受队列数据ssc.queueStream(queue)---->3.累加数字求和---->4.启动任务---->5.循环向queue写入数据（延迟2 s）---->6.阻塞任务

代码：

```java
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object SparkStreaming02_RDDStream {

    def main(args: Array[String]): Unit = {

        //1.初始化Spark配置信息
        val conf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")

        //2.初始化SparkStreamingContext
        val ssc = new StreamingContext(conf, Seconds(4))

        //3.创建RDD队列
        val rddQueue = new mutable.Queue[RDD[Int]]()

        //4.创建QueueInputDStream
        // oneAtATime = true 默认，一次读取队列里面的一个数据
        // oneAtATime = false， 按照设定的时间，读取队列里面数据
        val inputDStream = ssc.queueStream(rddQueue, oneAtATime = false)

        //5.处理队列中的RDD数据
        val sumDStream = inputDStream.reduce(_+_)

        //6.打印结果
        sumDStream.print()

        //7.启动任务
        ssc.start()

        //8.循环创建并向RDD队列中放入RDD
        for (i <- 1 to 5) {
            rddQueue += ssc.sparkContext.makeRDD(1 to 5)
            Thread.sleep(2000)
        }

        ssc.awaitTermination()
```

## 3.2 自定义数据源

### 3.2.1 用法及说明

**需要继承Receiver，并实现onStart、onStop方法来自定义数据源采集。**

需求：自定义数据源，实现监控某个端口号，获取该端口号内容。
1）使用自定义的数据源采集数据

```java
import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming03_CustomerReceiver {

    def main(args: Array[String]): Unit = {

        //1.初始化Spark配置信息
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")

        //2.初始化SparkStreamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(5))

        //3.创建自定义receiver的Streaming
        val lineDStream = ssc.receiverStream(new CustomerReceiver("hadoop102", 9999))

        //4.将每一行数据做切分，形成一个个单词
        val wordDStream = lineDStream.flatMap(_.split(" "))

        //5.将单词映射成元组（word,1）
        val wordToOneDStream = wordDStream.map((_, 1))

        //6.将相同的单词次数做统计
        val wordToSumDStream = wordToOneDStream.reduceByKey(_ + _)

        //7.打印
        wordToSumDStream.print()

        //8.启动SparkStreamingContext
        ssc.start()
        ssc.awaitTermination()
```

2）自定义数据源

```java
/**
* @param host ： 主机名称
 * @param port ： 端口号
 *  Receiver[String] ：返回值类型：String
 *  StorageLevel.MEMORY_ONLY： 返回值存储方式
 */
class CustomerReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

    // 最初启动的时候，调用该方法，作用为：读数据并将数据发送给Spark
    override def onStart(): Unit = {

        new Thread("Socket Receiver") {
            override def run() {
                receive()
            }
        }.start()
    }

    // 读数据并将数据发送给Spark
    def receive(): Unit = {

        // 创建一个Socket
        var socket: Socket = new Socket(host, port)

        // 创建一个BufferedReader用于读取端口传来的数据
        val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))

        // 读取数据
        var input: String = reader.readLine()

        //当receiver没有关闭并且输入数据不为空，则循环发送数据给Spark
        while (!isStopped() && input != null) {
            store(input)
            input = reader.readLine()
        }

        // 如果循环结束，则关闭资源
        reader.close()
        socket.close()

        //重启接收任务
        restart("restart")
    }

    override def onStop(): Unit = {}
```

## 3.3 Kafka数据源（面试、开发重点）

### 3.3.1 版本选型

**Receiver API和Direct API**

Receiver API：需要一个专门的Executor去接收数据，然后发送给其他Executor做计算。

存在的问题是接收Executor和计算Executor速度会有所不同，在接收Executor速度大于计算Executor时，会到时计算数据的内存溢出。

Direct API：是由计算的Executor来主动消费Kafka的数据，速度由自身控制。

**注意：目前spark3.0.0以上版本只有Direct模式。**

总结：不同版本的offset存储位置

 0-8 Receiver API offset默认存储在：Zookeeper中

 0-8 Direct API offset默认存储在：Check Point

手动维护：MySQL等有事务的存储系统

 **0-10 Direct API offset默认存储在：**_consumer_offsets系统主题

手动维护：My Sql等有事务存储系统

### 3.3.2 Kafka 0-10 Direct模式

1）需求：通过Spark Streaming从Kafka读取数据，并将读取过来的数据做简单计算，最终打印到控制台。

2）导入依赖

```xml
<dependency>
     <groupId>org.apache.spark</groupId>
     <artifactId>spark-streaming-kafka-0-10_2.12</artifactId>
     <version>3.0.0</version>
</dependency>
```

3）编写代码

**//读取****Kafk**a数据创建DStream****
      **KafkaUtils.createDirectStream //调用KafkaUtils的createdDirectStream方法**

```java
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming04_DirectAuto {

    def main(args: Array[String]): Unit = {

        //1.创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setAppName("sparkstreaming").setMaster("local[*]")

        //2.创建StreamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        //3.定义Kafka参数：kafka集群地址、消费者组名称、key序列化、value序列化
        val kafkaPara: Map[String, Object] = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
            ConsumerConfig.GROUP_ID_CONFIG -> "atguiguGroup",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
        )

        //4.读取Kafka数据创建DStream
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent, //优先位置
            ConsumerStrategies.Subscribe[String, String](Set("testTopic"), kafkaPara)// 消费策略：（订阅多个主题，配置参数）
        )

        //5.将每条消息的KV取出
        val valueDStream: DStream[String] = kafkaDStream.map(record => record.value())

        //6.计算WordCount
        valueDStream.flatMap(_.split(" "))
            .map((_, 1))
            .reduceByKey(_ + _)
            .print()

        //7.开启任务
        ssc.start()
        ssc.awaitTermination()

```

