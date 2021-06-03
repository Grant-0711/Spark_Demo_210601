# 一、常见API

## 1.SparkConf

### 1.1作用

是Spark应用的配置对象。用来加载Spark的各种配置参数，用户也可以使用这个对象设置自定义的参数！自定义的参数有更高的优先级！



可以使用链式编程，调用setter方法！

### 1.2 创建

new SparkConf

### 1.3 常见方法 

setMaster：master一般理解为分布式系统的管理进程！setMaster设置当前的Job需要提交到哪个集群！

​						通常写的是集群master的提交的url(和master通信的Url)

setAppName: 设置当前的应用名称

| masterurl                     | 模式                 |
| ----------------------------- | -------------------- |
| local /  local[n]  / local[*] | 本地                 |
|                               | 独立模式(standalone) |
| yarn                          | YARN模式             |

## 2.SparkContext

### 2.1 作用

- 使用Spark功能的核心入口
- 代表和Spark集群的连接
- 可以用来创建RDD,累加器，广播变量等编程需要的模型



注意： 在一个JVM中，只能创建一个SparkContext!

​			可以调用Stop，在创建新的SparkContext之前！



### 2.2 创建

```
class SparkContext(config: SparkConf)
```

# 二、核心概念

## 1.partition

MapReduce:  有shuffle

​			MapTask:       context.write(k,v)------->分区-------->排序----------->溢写......

​									combine

​		

​			分区的目的：①分类，将相同的数据分到一个区

​											每个ReduceTask都会处理一个分区的数据

​									②并行运算



Kafka:  有Topic，有Partition

​			分区的目的：  ①方便主题的扩展

​									 ②分区多，同时运行多个消费者线程并行消费，提升消费速率



Spark: 分区。 RDD（数据的集合），对集合进行分区！

​			分区的目的：  ①分类(使用合适的分区器)

​									 ②并行运算(主要目的)



## 2.术语

| 概念        | 解释                                                         |
| ----------- | ------------------------------------------------------------ |
| client      | 申请运行Job任务的设备或程序                                  |
| server      | 处理客户端请求，响应结果                                     |
| master      | 集群的管理者，负责处理提交到集群上的Job，类比为RM            |
| worker      | 工作者！实际负责接受master分配的任务，运行任务，类比为NM     |
| driver      | 创建SparkContext的程序，称为Driver程序                       |
| executor    | 计算器，计算者。是一个进程，负责Job核心运算逻辑的运行！      |
| task        | 计算任务！ task是线程级别！在一个executor中可以同时运行多个task |
| 并行度      | 取决于executor申请的core数                                   |
| application | 可以提交SparkJob的应用程序，在一个application中，可以调用多次行动算子，每个行动算子都是一个Job! |
| Job         | 一个Spark的任务，在一个Job中，Spark又会将Job划分为若干阶段(stage)，在划分阶段时，会使用DAG调度器，将算子按照特征(是否shuffle)进行划分。 |
| stage       | 阶段。一个Job的stage的数量=  shuffle算子的个数+1。 只要遇到会产生shuffle的算子，就会产生新的阶段！ 阶段划分的意义： 同一个阶段的每个分区的数据，可以交给1个Task进行处理！ |



转换算子(方法)： 将一个RDD 转换为 另一个RDD

​							方法传入RDD，返回RDD，就是转换算子！

行动算子： 将一个RDD提交为一个Job

​						   方法传入RDD，返回值不是RDD，通常都是行动算子！

​							只有行动算子会提交Job!

# 三、安装Spark

## 1.本地模式

### 1.1 windows本地模式

安装：将spark-3.0.0-bin-hadoop3.2.tgz解压到非中文无空格目录！

测试： 

spark-shell:  交互式环境

```scala
 val result = sc.textFile("input").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().mkString(",")
```

spark-submit:  用来提交jar包

用法： spark-submit  [options]   jar包   jar包中类的参数

```shell
 spark-submit --master local  --class com.atguigu.spark.day01.WordCount1  spark-1.0-SNAPSHOT.jar  input
```



### 1.2 linux的本地模式

安装：将spark-3.0.0-bin-hadoop3.2.tgz解压到非中文无空格目录！

测试： 

spark-shell:  交互式环境

```scala
 val result = sc.textFile("input").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().mkString(",")
```

spark-submit:  用来提交jar包

用法： spark-submit  [options]   jar包   jar包中类的参数

```shell
 spark-submit --master local  --class com.atguigu.spark.day01.WordCount1  spark-1.0-SNAPSHOT.jar  input
```



## 2.独立模式(standalone)

### 2.1 安装

①在一台机器安装spark

②编辑$SPARK_HOME/conf/slaves

```
#在哪些机器启动worker
hadoop102
hadoop103
hadoop104
```

③编辑 $SPARK_HOME/conf/spark-env.sh

```
#告诉每个worker，集群中的master在哪个机器
SPARK_MASTER_HOST=hadoop103
##告诉每个worker，集群中的master绑定的rpc端口号
SPARK_MASTER_PORT=7077
```

④分发到集群

⑤启动

```
sbin/start-all.sh
```



### 2.2 常见的端口号

| 进程          | 端口号 | 协议 |
| ------------- | ------ | ---- |
| master        | 7077   | RPC  |
| master        | 8080   | HTTP |
| worker        | 8081   | HTTP |
| Job运行时监控 | 4040   | HTTP |



### 2.3 测试

测试： 

spark-shell --master spark://hadoop103:7077:  交互式环境

```scala

```



spark-submit:  用来提交jar包

用法： spark-submit  [options]   jar包   jar包中类的参数

```shell

```



### 2.4 --deploy-mode

client(默认)：  会在Client本地启动Driver程序！

```

```

cluster:    会在集群中选择其中一台机器启动Driver程序！

```

```



区别：

①Driver程序运行位置的不同会影响结果的查看！

​				client： 将某些算子的结果收集到client端！

​								要求client端Driver不能中止！

​				cluster（生产）:  需要在Driver运行的executor上查看日志！



②client：  jar包只需要在client端有！

​	cluster:  保证jar包可以在集群的任意台worker都可以读到！



## 3.配置历史服务

作用：在Job离线期间，依然可以通过历史服务查看之前生成的日志！

①配置 SPARK_HOME/conf/spark-defaults.conf

影响启动的spark应用

```
spark.eventLog.enabled true
#需要手动创建
spark.eventLog.dir hdfs://hadoop102:9820/sparklogs
```



②配置 SPARK_HOME/conf/spark-env.sh

影响的是spark的历史服务进程

```
export SPARK_HISTORY_OPTS="
-Dspark.history.ui.port=18080 
-Dspark.history.fs.logDirectory=hdfs://hadoop102:9820/sparklogs
-Dspark.history.retainedApplications=30"
```



③分发以上文件，重启集群

④启动历史服务进程

```
sbin/start-historyserver.sh
```



## 4.spark on yarn

YARN： 集群！

Spark:  相对于YARN来说，就是一个客户端，提交Job到YARN上运行！

​			 

MR ON YARN提交流程：  ①Driver 请求 RM，申请一个applicatiion

​											 ②RM接受请求，此时，RM会首先准备一个Container运行 ApplicationMaster

​											 ③ ApplicationMaster启动完成后，向RM进行注册，向RM申请资源运行

​													其他任务

​												Hadoop MR :  申请Container运行MapTask，ReduceTask

​												Spark：   申请Container运行 Executor



只需要选择任意一台可以连接上YARN的机器，安装Spark即可！



### 4.1 配置

确认下YARN的 yarn-site.xml配置中

```xml
<!--是否启动一个线程检查每个任务正使用的物理内存量，如果任务超出分配值，则直接将其杀掉，默认是true -->
<property>
     <name>yarn.nodemanager.pmem-check-enabled</name>
     <value>false</value>
</property>

<!--是否启动一个线程检查每个任务正使用的虚拟内存量，如果任务超出分配值，则直接将其杀掉，默认是true -->
<property>
     <name>yarn.nodemanager.vmem-check-enabled</name>
     <value>false</value>
</property>

```

```xml
<property>
        <name>yarn.log.server.url</name>
        <value>http://hadoop102:19888/jobhistory/logs</value>
    </property>
```



分发 yarn-site.xml，重启YARN！



编辑 SPARK_HOME/conf/spark-env.sh

```shell
YARN_CONF_DIR=/opt/module/hadoop-3.1.3/etc/hadoop

export SPARK_HISTORY_OPTS="
-Dspark.history.ui.port=18080 
-Dspark.history.fs.logDirectory=hdfs://hadoop102:9820/sparklogs
-Dspark.history.retainedApplications=30"
```

编辑 SPARK_HOME/conf/spark-defaults.conf

```
spark.eventLog.enabled true
#需要手动创建
spark.eventLog.dir hdfs://hadoop102:9820/sparklogs
#spark历史服务的地址和端口
spark.yarn.historyServer.address=hadoop103:18080
spark.history.ui.port=18080
```



### 4.2 测试

spark-shell --master yarn:  交互式环境

```scala

```

spark-submit:  用来提交jar包

用法： spark-submit  [options]   jar包   jar包中类的参数

```shell

```

```

```



# 四、RDD

## 1.RDD的介绍

RDD：  RDD在Driver中封装的是计算逻辑，而不是数据！

​			  使用RDD编程后，调用了行动算子后，此时会提交一个Job，在提交Job时会划分Stage，划分之后，将每个阶段的每个分区使用一个Task进行计算处理！

​			Task封装的就是计算逻辑！ 

​			Driver将Task发送给Executor执行，Driver只是将计算逻辑发送给了Executor!Executor在执行计算逻辑时，此时发现，我们需要读取某个数据，举例(textFile)

​		

​		RDD真正被创建是在Executor端！

​		移动计算而不是移动数据，如果读取的数据是HDFS上的数据时，此时HDFS上的数据以block的形式存储在DataNode所在的机器！

​		如果当前Task要计算的那片数据，恰好就是对应的那块数据，那块数据在102，当前Job在102,104启动了Executor，当前Task发送给102更合适！省略数据的网络传输，直接从本地读取块信息！



## 2.RDD的核心特征

```
 一组分区
 - A list of partitions
 
 private var partitions_ : Array[Partition] = _
 
 #计算分区，生成一个数组，总的分区数
 protected def getPartitions: Array[Partition]
 
 每个Task通过compute读取分区的数据
*  - A function for computing each split
 def compute(split: Partition, context: TaskContext): Iterator[T]
 
 
 记录依赖于其他RDD的依赖关系，用于容错时，重建数据
*  - A list of dependencies on other RDDs

针对RDD[(k,v)]，有分区器！
*  - Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned)


针对一些数据源，可以设置数据读取的偏好位置，用来将task发送给指定的节点，可选的
*  - Optionally, a list of preferred locations to compute each split on (e.g. block locations for
*    an HDFS file)
```



## 3.创建RDD

RDD怎么来： ①new

​								直接new

​								调用SparkContext提供的方法

​						 ②由一个RDD转换而来



### 3.1 makeRDD

从集合中构建RDD

```scala
// 返回ParallelCollectionRDD
def makeRDD[T: ClassTag](
      seq: Seq[T],   
      numSlices: Int = defaultParallelism): RDD[T] = withScope {
    parallelize(seq, numSlices)
  }
```



```
val rdd1: RDD[Int] = sparkContext.makeRDD(list)
    //等价
val rdd2: RDD[Int] = sparkContext.parallelize(list)
```



#### 3.1.1 分区数

numSlices: 控制集合创建几个分区！



在本地提交时，defaultParallelism（默认并行度）由以下参数决定：

```scala
override def defaultParallelism(): Int =
// 默认的 SparkConf中没有设置spark.default.parallelism
  scheduler.conf.getInt("spark.default.parallelism", totalCores)
```

默认defaultParallelism=totalCores(当前本地集群可以用的总核数)，目的为了最大限度并行运行！

​											standalone / YARN模式， totalCores是Job申请的总的核数！



本地集群总的核数取决于  ： Local[N]

​				local:  1核

​				local[2]: 2核

​				local[*] : 所有核



#### 3.1.2 分区策略

```scala
 val slices = ParallelCollectionRDD.slice(data, numSlices).toArray

/*
		seq: List(1, 2, 3, 4, 5)
		numSlices: 4
*/
def slice[T: ClassTag](seq: Seq[T], numSlices: Int): Seq[Seq[T]] = {
    if (numSlices < 1) {
      throw new IllegalArgumentException("Positive number of partitions required")
    }
   
    
    /*
    	length:5
        numSlices: 4
        
        [0,4)
        { (0,1),(1,2),(2,3),(3,5)   }
    */
    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
        [0,4)
      (0 until numSlices).iterator.map { i =>
        val start = ((i * length) / numSlices).toInt
        val end = (((i + 1) * length) / numSlices).toInt
        (start, end)
      }
    }
    seq match {
      // Range特殊对待
      case _ =>
        // ｛1，2，3，4，5｝
        val array = seq.toArray // To prevent O(n^2) operations for List etc
        
        //{ (0,1),(1,2),(2,3),(3,5)   }
        positions(array.length, numSlices).map { case (start, end) =>
            {1}
            {2}
            {3}
            {4,5}
            array.slice(start, end).toSeq
        }.toSeq
    }
  }
```

总结：  ParallelCollectionRDD在对集合中的元素进行分区时，大致是平均分。如果不能整除，后面的分区会多分！



### 3.2 textfile

textfile从文件系统中读取文件，基于读取的数据，创建HadoopRDD！



#### 3.2.1 分区数

```scala
def textFile(
      path: String,
      minPartitions: Int = defaultMinPartitions): RDD[String] = withScope {
    assertNotStopped()
    hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      minPartitions).map(pair => pair._2.toString).setName(path)
  }
```



defaultMinPartitions:

```scala
// 使用defaultParallelism(默认集群的核数) 和 2取最小
def defaultMinPartitions: Int = math.min(defaultParallelism, 2)
```



defaultMinPartitions和minPartitions 不是最终 分区数，但是会影响最终分区数！



最终分区数，取决于切片数！

#### 3.2.2 分区策略

```scala
override def getPartitions: Array[Partition] = {
    val jobConf = getJobConf()
    // add the credentials here as this can be called before SparkContext initialized
    SparkHadoopUtil.get.addCredentials(jobConf)
    try {
        // getInputFormat() 获取输入格式
      val allInputSplits = getInputFormat(jobConf).getSplits(jobConf, minPartitions)
      val inputSplits = if (ignoreEmptySplits) {
        allInputSplits.filter(_.getLength > 0)
      } else {
        allInputSplits
      }
      
        // 切片数 就是分区数
      val array = new Array[Partition](inputSplits.size)
      for (i <- 0 until inputSplits.size) {
        array(i) = new HadoopPartition(id, i, inputSplits(i))
      }
      array
    }
  }
```



切片策略：

```java
public InputSplit[] getSplits(JobConf job, int numSplits)
    throws IOException {
    StopWatch sw = new StopWatch().start();
    FileStatus[] files = listStatus(job);
    
    // Save the number of input files for metrics/loadgen
    job.setLong(NUM_INPUT_FILES, files.length);
    
    // 计算所有要切的文件的总大小
    long totalSize = 0;                           // compute total size
    for (FileStatus file: files) {                // check we have valid files
      if (file.isDirectory()) {
        throw new IOException("Not a file: "+ file.getPath());
      }
      totalSize += file.getLen();
    }

    // goalSize： 目标大小  numSplits=minPartitions
    long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
    // minSize: 默认1
    long minSize = Math.max(job.getLong(org.apache.hadoop.mapreduce.lib.input.
      FileInputFormat.SPLIT_MINSIZE, 1), minSplitSize);

    // generate splits
    ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
    NetworkTopology clusterMap = new NetworkTopology();
    
    // 遍历要切片的文件
    for (FileStatus file: files) {
      Path path = file.getPath();
      long length = file.getLen();
        
        //如果文件不为空，依次切片
      if (length != 0) {
        FileSystem fs = path.getFileSystem(job);
        BlockLocation[] blkLocations;
        if (file instanceof LocatedFileStatus) {
          blkLocations = ((LocatedFileStatus) file).getBlockLocations();
        } else {
          blkLocations = fs.getFileBlockLocations(file, 0, length);
        }
          
          //判断文件是否可切，如果可切，依次切片
        if (isSplitable(fs, path)) {
            
            // 获取块大小  默认128M
          long blockSize = file.getBlockSize();
            
            // 获取片大小
          long splitSize = computeSplitSize(goalSize, minSize, blockSize);

          long bytesRemaining = length;
          while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
            String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations,
                length-bytesRemaining, splitSize, clusterMap);
            splits.add(makeSplit(path, length-bytesRemaining, splitSize,
                splitHosts[0], splitHosts[1]));
            bytesRemaining -= splitSize;
          }

          if (bytesRemaining != 0) {
            String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations, length
                - bytesRemaining, bytesRemaining, clusterMap);
            splits.add(makeSplit(path, length - bytesRemaining, bytesRemaining,
                splitHosts[0], splitHosts[1]));
          }
        } else {
            // 文件不可切，整个文件作为1片
          String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations,0,length,clusterMap);
          splits.add(makeSplit(path, 0, length, splitHosts[0], splitHosts[1]));
        }
          // 文件为空，空文件单独切一片
      } else { 
        //Create empty hosts array for zero length files
        splits.add(makeSplit(path, 0, length, new String[0]));
      }
    }
    sw.stop();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Total # of splits generated by getSplits: " + splits.size()
          + ", TimeTaken: " + sw.now(TimeUnit.MILLISECONDS));
    }
    return splits.toArray(new FileSplit[splits.size()]);
  }
```



片大小的计算：

```java
long splitSize = computeSplitSize(goalSize, minSize, blockSize);

// 默认blockSize=128M    goalSize： 46
return Math.max(minSize, Math.min(goalSize, blockSize));
```



总结：  一般情况下，blockSize =  splitSize，默认文件有几块，切几片！

## 4.RDD的算子分类

单Value类型的RDD：  RDD[value]

K-V类型的RDD：  RDD[(k,v)]

双Value类型的RDD：   RDD[value1]      RDD[value2]

### 4.1 单Value类型的RDD使用算子

#### 4.1.1map

```scala
def map[U: ClassTag](f: T => U): RDD[U] = withScope {
    // 当f函数存在闭包时，将闭包进行清理，确保使用的闭包变量可以被序列化，才能发给task
    val cleanF = sc.clean(f)
    
    // iter 使用scala集合中的迭代器，调用 map方法，迭代集合中的元素，每个元素都调用 f
    new MapPartitionsRDD[U, T](this, (_, _, iter) => iter.map(cleanF))
  }
```

MapPartitionsRDD





#### 4.1.2 mapPartitions

```scala
def mapPartitions[U: ClassTag](
      f: Iterator[T] => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = withScope {
    val cleanedF = sc.clean(f)
    new MapPartitionsRDD(
      this,
      (_: TaskContext, _: Int, iter: Iterator[T]) => cleanedF(iter),
      preservesPartitioning)
  }
```

MapPartitionsRDD



```scala
//分区整体调用一次函数
mapPartitions:  (_: TaskContext, _: Int, iter: Iterator[T]) => cleanedF(iter)

// 分区中的每一个元素，调用一次f（函数）
map:  (this, (_, _, iter) => iter.map(cleanF)
```

区别：

①mapPartitions(批处理) 和 map（1对1）

②某些场景下，只能使用mapPartitions

​			将一个分区的数据写入到数据库中！

③ map（1对1） ，只能对每个元素都进行map处理

​		mapPartitions(批处理)，可以对一个分区的数据进行任何类型的处理，例如filter等其他算子都行！

​		返回的记录可以和之前输入的记录个数不同！



#### 4.1.3 mapPartitionsWithIndex

mapPartitions 为每个分区的数据提供了一个对应分区的索引号。



#### 4.1.4 flatMap

扁平化



#### 4.1.5 glom

glom将一个分区的数据合并到一个 Array中，返回一个新的RDD

```
def glom(): RDD[Array[T]] = withScope {
  new MapPartitionsRDD[Array[T], T](this, (_, _, iter) => Iterator(iter.toArray))
}
```



#### 4.1.6 shuffle

shuffle：  在Hadoop的MR中，shuffle意思为混洗，目的是为了在MapTask和ReduceTask传递数据！

​					在传递数据时，会对数据进行分区，排序等操作！



​					当Job有reduce阶段时，才会有shuffle!



Spark中的shuffle：

①spark中只有特定的算子会触发shuffle，shuffle会在不同的分区间重新分配数据！

​		如果出现了shuffle，会造成需要跨机器和executor传输数据，这样会造成低效和额外的资源消耗！



②和Hadoop的shuffle不同的时，数据分到哪些区是确定的，但是在区内的顺序不一定有序！

​	如果希望shuffle后的数据有序，可以以下操作：

​		a) 调用mapPartitions,对每个分区的数据，进行手动排序！

​		b)repartitionAndSortWithinPartitions

​		c)sortBy

​		

③什么操作会导致shuffle

​		a）重新分区的算子 ： reparition, collase

​		b)  xxxBykey类型的算子，除了 count(统计)ByKey

​		c）  join类型的算子，例如[`cogroup`](http://spark.apache.org/docs/latest/rdd-programming-guide.html#CogroupLink) and [`join`](http://spark.apache.org/docs/latest/rdd-programming-guide.html#JoinLink).



④在Spark中，shuffle会带来性能消耗，主要涉及  磁盘IO,网络IO，对象的序列化和反序列化！

​	在Spark中，基于MR中提出的MapTask和ReduceTask概念，spark也将shuffle中组织数据的task称为

​	maptask,将聚合数据的task称为reduceTask!



​	maptask和spark的map算子无关，reducetask和reduce算子无关！



​	Spark的shuffle，mapTask将所有的数据先缓存如内存，如果内存不足，将数据基于分区排序后，溢写到磁盘！ ReduceTask读取相关分区的数据块，组织数据！

​	mapTask端在组织数据时，如果内存不够，导致磁盘溢写，触发GC！



​	Spark的shuffle同时会在磁盘上产生大量的溢写的临时文件，这个临时文件会一直保留，直到后续的RDD完全不需要使用！此举是为了避免在数据容错时，重新计算RDD，重新产生shuffle文件！



​	长时间运行的Spark的Job，如果在shuffle中产生大量的临时的磁盘文件，会占用大量的本地磁盘空间，可以通过spark.local.dir设置本地数据保存的目录！



总结： ①Spark的shuffle和Hadoop的shuffle目的都是为了 在不同的task交换数据！

​			 ②Spark的shuffle借鉴了hadoop的shuffle，但是在细节上略有不同

​					hadoop的shuffle：  数据被拉取到ReduceTask端时，会经过排序！

​					在Spark中，shuffle在ReduceTask端，拉取数据后不会排序！

​			③shuffle会消耗性能，因此能避免就避免，避免不了，采取一些优化的策略！

​	

#### 4.1.7 groupby

```
def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] = withScope {
  groupBy[K](f, defaultPartitioner(this))
}
```

将RDD中的元素通过一个函数进行转换，将转换后的类型作为KEY，进行分组！

#### 4.1.8 defaultPartitioner(this)

defaultPartitioner： 为类似cogroup-like类型的算子，选择一个分区器！在一组RDD中，选择一个分区器！



如何确定新的RDD的分区数：  

​			如果设置了并行度，使用并行度作为分区数，否则使用上游最大分区数

​			

如何确定分区器：

​			在上述确定了分区数后，就默认使用上游最大分区数所在RDD的分区器！如果分区器可以用，就使用！

​			

​			否则，就使用HashPartitioner，HashPartitioner使用上述确定的分区数，进行分区！





总结：  默认不设置并行度，取上游最大分区数，作为下游分区数。

​			 默认取上游最大分区数的分区器，如果没有，就使用HashPartitioner!



```scala
def defaultPartitioner(rdd: RDD[_], others: RDD[_]*): Partitioner = {
    //  将所有的RDD，放入到一个Seq中
    val rdds = (Seq(rdd) ++ others)
    /*
    	_.partitioner:  rdd.partitioner   =  Option[Partitioner]
    	
    	_.partitioner.exists(_.numPartitions > 0) 返回true: 
    			_.partitioner 不能为 none ,代表 RDD有分区器
    			且
    			分区器的总的分区数 > 0 
    			
   		hasPartitioner: Seq[Rdd] :  都有Partitioner，且分区器的总的分区数 > 0 
    			
    */  
    val hasPartitioner = rdds.filter(_.partitioner.exists(_.numPartitions > 0))

    // 取上游分区数最大的RDD  hasMaxPartitioner：要么为none，要么是拥有最大分区数和分区器的RDD
    val hasMaxPartitioner: Option[RDD[_]] = if (hasPartitioner.nonEmpty) {
      Some(hasPartitioner.maxBy(_.partitions.length))
    } else {
      None
    }

    // 如果设置了默认并行度，defaultNumPartitions=默认并行度，否则等于上游最大的分区数！
    val defaultNumPartitions = if (rdd.context.conf.contains("spark.default.parallelism")) {
      rdd.context.defaultParallelism
    } else {
      rdds.map(_.partitions.length).max
    }

    // If the existing max partitioner is an eligible one, or its partitions number is larger
    // than or equal to the default number of partitions, use the existing partitioner.
    /*
    	如果上游存在有最大分区数的RDD有分区器，且(这个分区器可用 或 默认分区数 <= 上游存在有最大分区数的RDD的分区数)  就使用 上游存在有最大分区数的RDD的分区器，否则，就new HashPartitioner(defaultNumPartitions)
    */
    if (hasMaxPartitioner.nonEmpty && (isEligiblePartitioner(hasMaxPartitioner.get, rdds) ||
        defaultNumPartitions <= hasMaxPartitioner.get.getNumPartitions)) {
      hasMaxPartitioner.get.partitioner.get
    } else {
      new HashPartitioner(defaultNumPartitions)
    }
  }
```



#### 4.1.9  Partitioner

```scala
abstract class Partitioner extends Serializable {
    // 数据所分的总区数
  def numPartitions: Int
    // 返回每个元素的分区号
  def getPartition(key: Any): Int
}
```



spark默认提供两个分区器：

HashPartitioner:  调用元素的hashCode方法，基于hash值进行分区！

```scala

```

RangeParitioner:   局限：针对数据必须是可以排序的类型！

​				将数据，进行采样，采样（水塘抽样）后确定一个范围(边界)，将RDD中的每个元素划分到边界中！

​				采样产生的边界，会尽可能保证RDD的元素在划分到边界后，尽可能均匀！





RangeParitioner 对比 HashPartitioner的优势：  一定程序上，可以避免数据倾斜！

#### 4.1.10 sample

```scala
/*
   withReplacement： 是否允许一个元素被重复抽样
   		true： PoissonSampler算法抽样
   		false： BernoulliSampler算法抽样
   		
   fraction： 抽取样本的大小。
   			withReplacement=true： fraction >=0 
   			withReplacement=false :  fraction : [0,1]
   			
   seed: 随机种子，种子相同，抽样的结果相同！
*/
def sample(
    withReplacement: Boolean,
    fraction: Double,
    seed: Long = Utils.random.nextLong): RDD[T]
```



#### 4.1.11 distinct

```scala

```

去重！有可能会产生shuffle，也有可能没有shuffle！

有shuffle，原理基于reduceByKey进行去重！



可以使用groupBy去重，之后支取分组后的key部分！

#### 4.1.12 依赖关系

narrow dependency： 窄依赖！

wide(shuffle) dependency： 宽（shuffle）依赖！ 宽依赖会造成shuffle！会造成阶段的划分！



#### 4.1.13 coalesce

```scala
def coalesce(numPartitions: Int, shuffle: Boolean = false,
             partitionCoalescer: Option[PartitionCoalescer] = Option.empty)
            (implicit ord: Ordering[T] = null)
    : RDD[T]
```

coalesce: 可以将一个RDD重新划分到若干个分区中！是一个重新分区的算子！

​			默认coalesce只会产生窄依赖！默认只支持将多的分区合并到少的分区！

​			如果将少的分区，扩充到多的分区，此时，coalesce产生的新的分区器依旧为old的分区数，不会变化！



​			如果需要将少的分区，合并到多的分区，可以传入 shuffle=true，此时会产生shuffle！



#### 4.1.14 repartition

repartition： 重新分区的算子！一定有shuffle！

​			如果是将多的分区，核减到少的分区，建议使用collase，否则使用repartition



#### 4.1.15 ClassTag

泛型为了在编译时，检查方法传入的参数类型是否复合指定的泛型（类型检查）



泛型在编译后，泛型会被擦除。 如果在Java代码中，泛型会统一使用Object代替，如果scala会使用Any代替！

在运行时，不知道泛型的类型！



针对Array类型，实例化一个数组时，必须知道当前的数组是个什么类型的数组！

java:   String []

scala：  Array[String]



Array和泛型共同组成数组的类型！ 在使用一些反射框架实例化一个数组对象时，必须指定数组中的泛型！

需要ClassTag,来标记Array类型在擦除前，其中的泛型类型！



#### 4.1.16 filter

```scala
def filter(f: T => Boolean): RDD[T] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[T, T](
      this,
      (_, _, iter) => iter.filter(cleanF),
      preservesPartitioning = true)
  }
```

将RDD中的元素通过传入的函数进行计算，返回true的元素可以保留！



#### 4.1.17 sortBy

```scala
def sortBy[K](
    f: (T) => K,
    ascending: Boolean = true,
    numPartitions: Int = this.partitions.length)
    (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] = withScope {
  this.keyBy[K](f)
      .sortByKey(ascending, numPartitions)
      .values
}
```

使用函数将RDD中的元素进行转换，之后基于转换的类型进行比较排序，排序后，再返回对应的转换之前的元素！



本质利用了sortByKey,有shuffle！

sortByKey在排序后，将结果进行重新分区时，使用RangePartitioner!



对于自定义类型进行排序：

​		①将自定义类型实现Ordered接口

​		②提供对自定义类型排序的Ordering



#### 4.1.18 pipe

pipe允许一个shell脚本来处理RDD中的元素！



在shell脚本中，可以使用READ函数读取RDD中的每一个数据，使用echo将处理后的结果输出！

每个分区都会调用一次脚本！

### 4.2 双Value类型

#### 4.2.1 intersection

两个RDD取交集，会产生shuffle！最终交集后RDD的分区数取决于上游RDD最大的分区数！



#### 4.2.2 union

两个RDD取并集（并不是数学上的并集，元素允许重复）。没有shuffle！

最终并集后的RDD的分区数，取决于所有RDD分区数的总和！



union 等价于  ++



#### 4.2.3 substract

两个RDD取差集，产生shuffle！取差集合时，会使用当前RDD的分区器和分区数！



#### 4.2.4 cartesian

两个RDD做笛卡尔积，不会产生shuffle！ 运算后的RDD的分区数=所有上游RDD分区数的乘积。



#### 4.2.5 zip

将两个RDD，相同分区，相同位置的元素进行拉链操作，返回一个(x,y)

要求： 两个RDD的分区数和分区中元素的个数必须相同！



#### 4.2.6 zipWithIndex

当前RDD的每个元素和对应的index进行拉链，返回(ele,index)



#### 4.2.7 zipPartitions

两个RDD进行拉链，在拉链时，可以返回任意类型的元素的迭代器！更加灵活！



### 4.3 key-value类型

key-value类型的算子，需要经过隐式转换将RDD[(k,v)]转换为 PairRDDFunctions,才可以调用以下算子

#### 4.3.1 reduceByKey

```
def reduceByKey(func: (V, V) => V): RDD[(K, V)] = self.withScope {
  reduceByKey(defaultPartitioner(self), func)
}
```

会在Map端进行局部合并（类似Combiner）

注意：合并后的类型必须和之前value的类型一致！



#### 4.3.2 aggregateByKey

```scala

def aggregateByKey[U: ClassTag]
(zeroValue: U)
(seqOp: (U, V) => U,
    combOp: (U, U) => U): RDD[(K, U)] = self.withScope {
  aggregateByKey(zeroValue, defaultPartitioner(self))(seqOp, combOp)
}
```



#### 4.3.3 foldByKey

```scala
def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)] = self.withScope {
  foldByKey(zeroValue, defaultPartitioner(self))(func)
}
```

foldByKey是aggregateByKey的简化版！

​		foldByKey在运算时，分区内和分区间合并的函数都是一样的！



如果aggregateByKey，分区内和分区间运行的函数一致，且zeroValue和value的类型一致，可以简化使用foldByKey！



#### 4.3.4  combineByKey

```scala

def combineByKey[C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C): RDD[(K, C)] = self.withScope {
  combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners)(null)
}
```



combineByKeyWithClassTag的简化版本，简化在没有提供ClassTag

combineByKey: 是为了向后兼容。兼容foldByKey类型的算子！



#### 4.3.5 4个算子的区别

```scala
/*
	用函数计算zeroValue
	分区内计算
	分区间计算
	
*/
rdd.combineByKey(v => v + 10, (zero: Int, v) => zero + v, (zero1: Int, zero2: Int) => zero1 + zero2)

//如果不需要使用函数计算zeroValue，而是直接传入，此时就可以简化为

rdd.aggregateByKey(10)(_ + _, _ + _)

//如果aggregateByKey的分区内和分区间运算函数一致，且zeroValue和value同一类型，此时可以简化为

rdd.foldByKey(0)(_ + _)

// 如果zeroValue为0，可以简化为reduceByKey

rdd.reduceByKey(_ + _)


//四个算子本质都调用了，只不过传入的参数进行了不同程度的处理
// 以上四个算子，都可以在map端聚合！
combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners)(null)

// 根据需要用，常用是reduceByKey
```

#### 4.3.6 partitionBy

使用指定的分区器对RDD进行重新分区！



自定义Partitioner，自定义类，继承Partitioner类！实现其中的getPartition()



#### 4.3.7 mapValues

将K-V类型RDD相同key的所有values经过函数运算后，再和Key组成新的k-v类型的RDD



#### 4.3.8 groupByKey

根据key进行分组！



#### 4.3.9 SortByKey

根据key进行排序！ 

​		针对自定义的类型进行排序！可以提供一个隐式的自定义类型的排序器Ordering[T]



#### 4.3.10 连接

Join:  根据两个RDD key，对value进行交集运算！





LeftOuterJoin:  类似 left join，取左侧RDD的全部和右侧key有关联的部分！

RightOuterJoin: 类似 right join，取右侧RDD的全部和左侧key有关联的部分！

FullOuterJoin： 取左右RDD的全部，及有关联的部分！

​		如果关联上，使用Some，如果关联不上使用None标识！

#### 4.3.11 cogroup

将左右两侧RDD中所有相同key的value进行聚合，返回每个key及对应values的集合！

将两侧的RDD根据key进行聚合，返回左右两侧RDD相同的values集合的RDD！



## 5.行动算子

### 5.1 区别

行动算子用来提交Job！和转换算子不同的时，转换算子一般是懒执行的！转换算子需要行动算子触发！



### 5.2 常见的行动算子

| 算子         | 解释                                                         | 备注                                                   |
| ------------ | ------------------------------------------------------------ | ------------------------------------------------------ |
| reduce       | 将RDD的所有的元素使用一个函数进行归约，返回归约后的结果      |                                                        |
| collect      | 将RDD的所有元素使用Array进行返回，收集到Driver               | 慎用！如果RDD元素过多，Driver端可能会OOM！             |
| count        | 统计RDD中元素的个数                                          |                                                        |
| take(n:Int)  | 取前N个元素                                                  | 慎用！如果取RDD元素过多，Driver端可能会OOM！           |
| takeOrdered  | 取排序后的前N个元素                                          | 慎用！如果取RDD元素过多，Driver端可能会OOM！           |
| first        | 返回第一个元素                                               |                                                        |
| aggregate    | 和aggregateByKey算子类似，不同的是zeroValue在分区间聚合时也会参与运算 |                                                        |
| fold         | 简化版的aggregate，分区内和分区间的运算逻辑一样              |                                                        |
| countByValue | 统计相同元素的个数                                           |                                                        |
| countByKey   | 针对RDD[(K,V)]类型的RDD，统计相同key对应的K-V的个数          |                                                        |
| foreach      | 遍历集合中的每一个元素，对元素执行函数                       | 特殊场景（向数据库写入数据），应该使用foreachPartition |
| save相关     | 将RDD中的数据保存为指定格式的文件                            | 保存为SequnceFile文件，必须是RDD[(K,V)]                |
| 特殊情况     | new RangeParitioner时，也会触发Job的提交！                   |                                                        |
|              |                                                              |                                                        |

## 6.序列化

### 6.1 序列化

如果转换算子存在闭包，必须保证闭包可以被序列化（使用外部变量可以被序列化）！

否则报错 Task not Serilizable，Job不会被提交！



解决：  ①闭包使用的外部变量 extends Serializable

​				②使用case class声明外部变量



特殊情况：  如果闭包使用的外部变量是某个类的属性或方法，此时这个类也需要被序列化！

​					

​				解决：  ①类也需要被序列化

​							②在使用某个类的属性时，可以使用局部变量 接收  属性

​							③在使用某个类的方法时，可以使用函数（匿名函数也可以） 代替 方法

### 6.2 Kryo

Kryo是Spark中高效的序列化框架！

使用： SparkConf.registerKryoClasses(Array(xxx))



## 7.依赖和血缘

### 7.1 依赖

查看： rdd.dependencies 查看



依赖描述的是当前RDD和父RDD之间，分区的对应关系！



```scala
abstract class Dependency[T] extends Serializable {
  def rdd: RDD[T]
}
```

```
NarrowDependency: 窄依赖
	RangeDependency： 1子多父
	OneToOneDependency： 1子1父
	
ShuffleDependency：宽依赖
```



作用： Spark内部使用！Spark任务在提交时，DAG调度器需要根据提交的当前RDD的dependencies 信息

​					来获取当前rdd的依赖类型，根据依赖类型划分阶段！

​				如果是ShuffleDependency，就会产生一个新的阶段！



### 7.2 血缘关系

RDD.toDebugString 查看



作用： Spark有容错机制！一旦某个RDD计算失败，或数据丢失，重新计算！重新计算时，需要根据血缘关系，从头到尾依次计算！ 是Spark容错机制的保障！



## 8.持久化

### 8.1 cache

cache的作用： 加速查询效率，缓存多使用内存作为存储设备！

​							在spark中，缓存可以用来缓存已经计算好的RDD，避免相同RDD的重复计算！



使用：   rdd.cache()    等价于   rdd.persist()  等价于  rdd.persist(Storage.MEMORY_ONLY)

​				rdd.persist(缓存的级别)



缓存会在第一个Job提交时，触发，之后相同的Job如果会基于缓存的RDD进行计算，优先从缓存中读取RDD！

​			

缓存多使用内存作为存储设备，内存是有限的，因此缓存多有淘汰策略，在Spark中，默认使用LRU（less recent use）算法淘汰缓存中的数据！



缓存因为不可靠性，所以并不会影响血缘关系！

带有shuffle的算子，会自动将MapTask阶段溢写到磁盘的文件，一致保存。如果有相同的Job使用到了这个文件，此时这个Map阶段可以被跳过，可以理解为使用到了缓存！



### 8.2 checkpoint

checkpoint为了解决cache的不可靠性！设置了checkpoint目录后，Job可以将当前RDD的数据，或Job的状态持久化到文件系统中！



作用： ①从文件系统中读取已经缓存的RDD

​			②当Job失败，需要重试时，从文件系统中获取之前Job运行的状态，基于状态恢复运行



使用：  SparkContext.setCheckpointDir (如果是在集群中运行，必须使用HDFS上的路径)

​				rdd.checkpoint()



checkpoint会在第一个 Job提交时，额外提交一个Job进行计算，如果要避免重复计算，可以和cache结合使用！





## 9.共享变量

### 9.1 广播变量

​		解决在Job中，共享只读变量！

​		可以将一个Job多个stage共有的大体量的数据，使用广播变量，发送给每台机器（executor），这个executor上的task都可以共享这份数据！



使用： var br= SparkContext.broadcast(v)

​		   br.value() //访问值



使用广播变量，必须是只读变量，不能修改！



### 9.2累加器

​		解决计数（类似MR中的Counter）以及sum求和的场景，比reduceByKey之类的算子高效！并行累加，之后在Driver端合并累加的结果！



使用：  ①Spark默认提供了数值类型的累加器，用户也可以自定义累加器，通过继承AccumulatorV2[IN,OUT]

​				②如果是自定义的累加器，需要进行注册

​							SparkContext.regist(累加器，名称)

​				③使用

​								累加：  add()

​								获取累加的结果： value()		

​					自定义累加器，必修实现以下方法：

​								reset(): 重置归0

​								isZero():判断是否归0

​								merge(): 合并相同类型的累加器的值



运行过程：  在Driver端创建一个累加器，基于这个累加器，每个Task都会先调用

​						Copy----->reset----->isZero

​							如果isZero返回false，此时就报错，Job没有提交，返回true，此时

​					将累加器随着Task序列化！

​						在Driver端，调用value方法合并所有（包括在Driver创建的）的累加器



注意： 累加器为了保证累加的精确性，必须每次在copy()后，将累加器重置归0

​			在算子中，不能调用value,只能在Driver端调用

​			累加器的输入和输出可以是不一样的类型



## 10.读写数据

读写文本：

```
		读：  SparkContext.textFile()
		写： RDD.saveAsTextFile
```



读写Object文件：

```
读：SparkContext.ObjectFile[T]()
写： RDD.saveAsObjectFile
```

读写SF文件：

```
读：SparkContext.SequenceFile[T]()
写： RDD.saveAsSequenceFileFile

RDD必须是K-V类型
```



读写JDBC：

```
读： new JDBCRDD()
写：  RDD.foreachePartition()
```



读写HBase：

```
读：    TableInputFormat
				RR:  key: ImmutableBytesWritable
					 value:  Result
					 
	   new NewHadoopRDD
	   
写：     TableOutputFormat
				RW： key： 随意，一般使用ImmutableBytesWritable存储rowkey
					 value:  Mutation (Put,Delete,Append)
					 
		RDD必须是K-V类型
			RDD.saveAsNewApiHadoopDataSet
```

# 五、SparkCore练习

## 1.数据

| 编号 | 字段名称           | 字段类型 | 字段含义                   |
| ---- | ------------------ | -------- | -------------------------- |
| 1    | date               | String   | 用户点击行为的日期         |
| 2    | user_id            | Long     | 用户的ID                   |
| 3    | session_id         | String   | Session的ID                |
| 4    | page_id            | Long     | 某个页面的ID               |
| 5    | action_time        | String   | 动作的时间点               |
| 6    | search_keyword     | String   | 用户搜索的关键词           |
| 7    | click_category_id  | Long     | 某一个商品品类的ID         |
| 8    | click_product_id   | Long     | 某一个商品的ID             |
| 9    | order_category_ids | String   | 一次订单中所有品类的ID集合 |
| 10   | order_product_ids  | String   | 一次订单中所有商品的ID集合 |
| 11   | pay_category_ids   | String   | 一次支付中所有品类的ID集合 |
| 12   | pay_product_ids    | String   | 一次支付中所有商品的ID集合 |
| 13   | city_id            | Long     | 城市  id                   |

```scala
//搜索行为，通过搜索关键字是否为null来判断
2019-07-17_95_26070e87-1ad7-49a3-8fb3-cc741facaddf_37_2019-07-17 00:00:02_手机_-1_-1_null_null_null_null_3

//点击的品类ID和产品ID 其中有一个不是-1，就代表是点击行为
2019-07-17_95_26070e87-1ad7-49a3-8fb3-cc741facaddf_48_2019-07-17 00:00:10_null_16_98_null_null_null_null_19

// 下单字段不为null，即为下单数据   下单了 15,13,5,11,8 品类的商品 和 99,2的产品
2019-07-17_38_6502cdc9-cf95-4b08-8854-f03a25baa917_24_2019-07-17 00:00:38_null_-1_-1_15,13,5,11,8_99,2_null_null_10

//支付字段不为null，即为支付数据  支付了43,4,8号产品，属于16,2品类
2019-07-17_58_d2949e34-fb81-45d8-acb9-d158cfb4097c_16_2019-07-17 00:03:23_null_-1_-1_null_null_16,2_43,4,8_2
```

四种行为：搜索，点击，下单，支付，每一条记录只能是四种行为的一种！

粒度：  每一条记录代表是一个用户一个session的一种行为！



## 2.需求一

### 2.1需求说明

需求方：   Top10热门品类

​					统计每一个品类的  点击数 ，下单数 ，支付数

​				先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数



### 2.2 实现思路

### 2.2.1 实现方式一

```sql
table UserVisitAction(
    date: String,//用户点击行为的日期
    user_id: Long,//用户的ID
    session_id: String,//Session的ID
    page_id: Long,//某个页面的ID
    action_time: String,//动作的时间点
    search_keyword: String,//用户搜索的关键词
    click_category_id: Long,//某一个商品品类的ID
    click_product_id: Long,//某一个商品的ID
    order_category_ids: String,//一次订单中所有品类的ID集合
    order_product_ids: String,//一次订单中所有商品的ID集合
    pay_category_ids: String,//一次支付中所有品类的ID集合
    pay_product_ids: String,//一次支付中所有商品的ID集合
    city_id: Long
)//城市 id

#统计每一个品类的  点击数 ，下单数 ，支付数
-- 分别求每一个品类的这三个参数
--点击数
select
		click_category_id,count(*) clickcount
from xx
where 过滤出点击的数据
group by click_category_id   //t1

--下单数
ordercount  //t2
--支付数
payCount   //t3

--点击结果 left Join

--click_category_id,clickcount,...  tmp

select
	click_category_id
from tmp
order by clickcount desc, ordercount desc ,payCount desc
limit 10


```

弊端： 分别求了3个参数，之后需要拼接，join 2次



希望改进： 避免Join!

```sql
--一次性求出三个数
select
	click_category_id,sum(clickcount) clickcount,
	sum(orderCount) orderCount,
	sum(payCount) payCount
from
(select
		click_category_id,count(*) clickcount,0 orderCount,0 payCount
from xx
where 过滤出点击的数据
group by click_category_id   //t1
union all
select
		click_category_id,count(*) orderCount,0 clickcount,0 payCount
from xx
where 过滤出下单的数据
group by click_category_id   //t2
union all
select
		click_category_id,count(*) payCount,0 clickcount,0 orderCount
from xx
where 过滤出支付的数据
group by click_category_id   //t3
 ) tmp
 group by click_category_id
  --排序
 order by clickcount desc, ordercount desc ,payCount desc
limit 10
 

```

优点： 解决了Join!

缺点：参数多，麻烦



#### 2.2.3 实现方式三

使用累加器实现！



#### 2.2.2 实现方式二



## 3.常用算子

```sql
select     // udf   一进一出    trim
	xxx,sum(),count()    //map
from xxx
where  xx       // filter
group by xx      // reduceByKey
having xxx      //filter
order by xxx    // sortby sortbyKey
limit xxx     // take 

            // join,leftOuterJoin
```



## 4.需求二



## 5.需求三

页面单跳： 由一个页面直接跳转到另一个页面

```
--单跳
A ----> B ------>C

-- 不符合
A-----> E ------>B
```

A--B的单跳： 1次



页面单跳转换率：  页面单跳次数 / 页面单跳中首页面的访问次数

上述案例：  页面单跳转换率 为 1 / 2 





步骤： ①求当前数据中所有的页面被访问的次数

​			 ②求每个页面单跳的次数

​						先求出页面的访问顺序

 						a) 求出每个用户，每个SESSION访问的记录，之后按照访问时间升序排序

```
a,session1,2020-11-11 11:11:11, A
a,session1,2020-11-11 11:12:11, B
a,session1,2020-11-11 11:11:09, C
a,session1,2020-11-11 11:13:11, A

a,session1, C---A---B---A

a,session2,2020-11-11 11:11:11, A
a,session2,2020-11-11 11:12:11, B 

a,session2, A----B
```

​						b)按照排序的结果求 页面的访问顺序,拆分页面单跳

```
a,session1, C---A---B---A
C---A
A---B
B---A

a,session2, A----B
A---B
```

​						c) 聚合所有的单跳记录，求出每个单跳的次数

```
C---A  1
A---B  2
B---A  1
```

​	     ③求转换率

```
A---3
B---2
C----1

----------
C---A  1
A---B  2
B---A  1

-----------
C---A  1/1
A---B  2/3
B---A  1/2
```









