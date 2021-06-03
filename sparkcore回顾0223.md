# 1.RDD概述

## 1.1 什么是RDD: RDD是弹性分布式数据集

RDD代表的是弹性.可分区.不可变.里面元素可以并行计算的集合
**弹性:**
存储的弹性: 中间结果是保存在内存中,如果内存不足自动保存在磁盘
容错的弹性: task执行失败会自动重试
计算的弹性: 如果数据出错,会根据RDD依赖关系重新计算得到数据
分区的弹性: 读取HDFS文件的时候,会根据文件的切片动态划分分区

**不可变:**
RDD不存储数据，RDD中只是封装数据的处理逻辑,如果想要修改数据,只能生成新的RDD		

**可分区:** 

数据是分布式存储,所以在计算的时候也是分布式
			

**并行计算:** 

分区的计算是并行的

## 1.2 RDD五大特性

**分区列表:** 数据是分布式存储,所以在计算的时候也是分布式,一般读取HDFS文件的时候一个文件切片对应一个RDD分区

**作用在每个分区上的计算函数:** task计算的时候并行的,每个task计算逻辑是一样的,只是处理的数据不一样

**依赖关系:** RDD不存储数据，RDD中只是封装数据的处理逻辑,如果数据出错需要根据依赖关系从头进行处理得到数据

**分区器:** 在shuffle阶段,需要通过分区器将相同key的数据聚在一个分区统一处理

**优先位置:** spark在分配task的时候最好将task分配到数据所在的数据,避免网络拉取数据影响效率

# 2.RDD编程

## 2.1  RDD的创建

### 2.1.1  通过集合创建

```scala
			sc.makeRDD(集合)
			sc.parallelize(集合)
			//makeRDD底层就是使用的parallelize
```

### 		

### 2.1.2 读取文件创建

```scala
sc.textFile(path)
			//在spark集群中如果有配置HADOOP_CONF_DIR,此时默认读取的是HDFS文件
				//读取HDFS文件的时候:
					sc.textFile("/.../..")
					sc.textFile("hdfs://hadoop102:8020/..../..")
					sc.textFile("hdfs:///../..")
				//读取本地文件:
					sc.textFile("file:///../...")
			//在spark集群中如果没有配置HADOOP_CONF_DIR,此时默认读取的是本地文件
				//读取HDFS文件的时候:
					sc.textFile("hdfs://hadoop102:8020/..../..")
				//读取本地文件:
					sc.textFile("/.../..")
					sc.textFile("file:///../...")
```

### 2.1.3  通过其他RDD衍生

```scala
	val rdd2 = rdd1.map/flatMap/..
```

## 2.分区规则

### 2.2.1  通过集合创建RDD: 

```scala
	val rdd = sc.parallelize(集合,slices=defaultParallelism)
```


通过集合创建RDD分区数:

#### 2.2.1.1 

在创建RDD的时候有设置slices参数值,此时RDD分区数 = slices参数值 【val RDD = sc.parallelize(List(...),4)】

#### 2.2.1.2 

在创建RDD的时候没有设置slices参数值,使用slices的默认值[defaultParallelism]

##### 2.2.1.2 .1  

有设置spark.default.parallelism参数值,此时RDD分区数 = spark.default.parallelism参数值

##### 2.2.1.2 .2  

没有设置spark.default.parallelism参数值
1.local模式
							如果master=local, ,此时RDD分区数 = 1
							如果master=local[N], ,此时RDD分区数 = N
							如果master=local[*], ,此时RDD分区数 = cpu个数
2.集群模式,此时RDD分区数 = 本次任务executor总核数

### 2.2.2  通过读取文件创建RDD

RDD分区数>= math.min(defaultParallelism,2)
读取文件创建的RDD的分区数 = 文件的切片数

### 2.2.3.通过其他RDD衍生的RDD

衍生出的RDD的分区数 = 父RDD的分区数

## 3.算子

spark算子分为两大类: 转换算子[transformation].行动算子[action]

### 3.1  转换算子: 只是对数据转换,不会触发job的执行

#### 3.1 .1 map

map(func: RDD元素类型=> B ): 映射 ******

map里面的函数是针对RDD每个元素进行操作,操作完成之后返回一个结果
map生成的新的RDD的元素的个数 = 原RDD元素个数

#### 3.1 .2 mapPartitions 

mapPartitions(func: 迭代器[RDD元素类型]=> 迭代器[B] ) ******
mapPartitions里面的函数是针对RDD每个分区进行操作,操作完成之后返回一个分区的结果数据
mapPartitions是RDD有多少分区就调用多少次
				

#### 3.1 .3 map与mapPartitions的区别: ******

##### 3.1 .3.1 针对的对象不一样

map里面的函数是针对RDD每个元素进行操作
mapPartitions里面的函数是针对RDD每个分区的所有数据的迭代器进行操作

##### 3.1 .3.2 返回结果不一样

map是操作一个元素返回一个结果,map生成的新的RDD的元素的个数 = 原RDD元素个数
mapPartitions是操作一个分区返回给一个分区的结果数据,mapPartitions生成的新的RDD的元素个数可能不等于原RDD元素个数

##### 3.1 .3.3 内存释放的时机不一样

map里面的函数是针对每个元素操作,元素操作完成之后就可以回收元素对应的内存
mapPartitions里面的函数是针对一个分区的的所有数据的迭代器操作,必须等到分区所有数据都处理完成之后才能进行内存回收[如果分区数据特别多可能会导致内存溢出,此时可以使用map代替]

#### 3.1.4 mapPartitionsWithIndex 

mapPartitionsWithIndex( (分区索引号,迭代器[RDD元素类型])=>迭代器[B] ) ******
mapPartitionsWithIndex与mapPartitions的区别:
mapPartitions里面的函数只有一个参数,参数就是每个分区所有数据的迭代器
mapPartitionsWithIndex里面的函数有两个参数,第一个参数是分区号,第二个参数是分区号对应分区的所有数据的迭代器

#### 3.1 .5 glom 

glom: 将分区所有数据封装成数组
glom生成的新RDD的元素个数 = RDD分区数

#### 3.1 .6 groupBy 

groupBy(func: RDD元素类型=> K):按照指定字段分组 ******
groupBy后续是按照函数返回值进行分组
groupBy会产生shuffle操作



​				3.1 .1filter(func: RDD元素类型=> Boolean ): 过滤 ******
​					filter是保留函数返回值为true的数据
​				3.1 .1sample(withReplacement,fraction): 采样【一般用于数据倾斜场景】******
​					withReplacement: 采样之后是否放回[true:放回,代表同一个元素可能被多次采样. false: 不放回,代表同一个元素最多被采样一次]【工作中一般设置为false】
​					3.1 .1fraction:
​						withReplacement=true,代表元素期望被采样的次数[>0]
​						withReplacement=false,代表每个元素被采样的概率[0,1] 【工作中一般设置0.1-0.2】
​				3.1 .1coalesce(maxPartitions,shuffle=false): 合并分区 ******
​					coalesce默认只能减少分区数,默认是没有shuffle操作的
​					coalesce如果想要增大分区，需要设置shuffle=true,此时会产生shuffle操作
​					coalesce工作中一般搭配filter使用
​				3.1 .1repartition(maxPartitions): 重分区 ******
​					repartition既可以增大分区也可以减少分区,都会产生shuffle
​					repartition底层就是使用的coalesce: coalesce(maxPartitions,shuffle=true)
​					coalesce与repartition的区别:
​						coalesce默认只能减少分区数,默认情况下没有shuffle
​						repartition既可以增大分区也可以减少分区,都有shuffle
​					如果想要减少分区推荐使用coalesce,因为不用shuffle
​					如果想要增大分区推荐使用repartition,因为简单
​				sortBy(func: RDD元素类型 => K,ascding=true ): 按照指定字段排序 ******
​					sortBy后续是按照函数返回值进行排序
​					可以通过ascding参数控制升降序
​					sortBy也会产生shuffle操作
​				pipe(脚本路径): 调用外部脚本
​					pipe是每个分区调用一次脚本
​					pipe会生成一个新的RDD,新RDD的元素是在脚本中通过echo返回的
​				intersection: 交集
​					intersection会产生shuffle操作
​				union: 并集
​					union没有shuffle操作
​					union产生的RDD的分区数 = union的两个RDD的分区数的总和
​				substract: 差集
​					substract会产生shuffle操作
​				zip: 拉链
​					两个RDD想要拉链必须分区数以及元素个数一致
​				flatMap(func: RDD元素类型=> 集合 ) = map+ flatten  ******
​					map里面的函数是针对RDD每个元素进行操作,操作完成之后返回一个集合
​					flatMap生成的新的RDD的元素的个数>=原RDD元素个数
​				partitionBy(partitioner): 根据指定分区器重新分区
​				自定义分区器:
​					1.定义class继承partitioner
​					2.重写抽象方法
​					3.创建自定义分区器对象,在shuffle算子中直接使用即可
​				reduceByKey(func: (Value类型,Value类型) => Value类型 )：根据key进行分组聚合 ******
​					reduceByKey里面的函数是针对每个组所有的value值进行聚合
​					reduceByKey里面的函数用于combiner以及reduce阶段
​				groupByKey: 直接根据key分组 ******
​				reduceByKey与groupByKey的区别: ******
​					reduceByKey有类似MR的combiner预聚合功能
​					groupByKey只是单纯的分组,没有类似MR的combiner预聚合功能,整体性能上要比reduceByKey要低
​				aggregateByKey(初始值:U)(seqOp:(U,value值类型)=>U,comOp: (U,U)=>U ): 根据key进行分组聚合
​					seqOp是combiner聚合逻辑
​					comOp是reduce聚合逻辑
​					初始值是seqOp函数在针对每个分组第一次聚合的时候,第一个参数的值 = 初始值
​				foldByKey(初始值:value值类型)(func:  (Value类型,Value类型) => Value类型 ): 根据key进行分组聚合
​					foldByKey里面的函数用于combiner以及reduce阶段
​					combiner计算阶段在每个组第一次聚合的时候,函数第一个参数的值 = 初始值
​				combineByKey(createCombine: value值类型 => B ,mergeValue: (B,value值类型)=>B ,mergeCombine: (B,B)=>B)
​					createCombine: 是在combiner阶段对每个组的第一个value值进行转换
​					mergeValue: combiner执行逻辑
​					mergeCombine： reduce聚合逻辑
​				reduceByKey.aggregateByKey.foldByKey.combineByKey的区别:
​					reduceByKey.foldByKey: combiner与reduce计算逻辑完全一样
​					aggregateByKey.combineByKey: combiner与reduce计算逻辑可以不一样
​					foldByKey.aggregateByKey: combiner计算阶段对每个组第一次聚合的时候,combiner函数第一个参数的值 = 初始值
​					reduceByKey.combineByKey: combiner计算阶段对每个组第一次聚合的时候,combiner函数第一个参数的值 = 每个组的第一个value值
​				sortByKey： 根据key排序  ******
​				mapValues(func: value值类型 => B ): 针对value值进行转换[类型的转换.值的转换]
​					mapValues里面的函数是针对每个元素的value值进行操作
​				join: 两个rdd的key相同的时候才能连接上
​					val rdd3:RDD[(key,(rdd1的value值,rdd2的value值))] = rdd1.join(rdd2)
​				leftOuterJoin: 两个rdd的key相同的时候才能连接上 + 左RDD不能连接上的数据
​				rightOuterJoin: 两个rdd的key相同的时候才能连接上 + 右RDD不能连接上的数据
​				fullOuterJoin: 两个rdd的key相同的时候才能连接上 + 左RDD不能连接上的数据 + 右RDD不能连接上的数据
​				cogroup: 类似全连接+ 分组
​					cogroup的结果: RDD[(key,(左RDD key对应的所有value值,右RDD key对应的所有value值))]
​			2.行动算子: 触发job的执行
​				reduce(func: (RDD元素类型,RDD元素类型)=>RDD元素类型): 对RDD所有元素聚合
​				collect:  收集RDD每个分区的数据返回给Driver ******
​					如果RDD的数据量比较大,Driver默认内存只有1G,所以可能会导致Driver的内存溢出。工作中一般需要配置Driver的内存为5-10G【通过--driver-memory】
​				count: 统计RDD元素个数
​				first: 获取RDD第一个元素
​				take: 获取RDD前N个元素
​				takeOrdered: 对RDD元素排序之后获取前N个
​				aggregate(初始值:U)(seqOp:(U,RDD元素类型)=>U,comOp: (U,U)=>U)： 对RDD所有元素聚合
​					初始值可以用于seqOp.comOp函数的第一次计算
​				fold(初始值:RDD元素类型)(func: (RDD元素类型,RDD元素类型)=>RDD元素类型)： 对RDD所有元素聚合
​				countByKey: 统计key出现的次数  ******
​					countByKey一般是结合sample用于数据倾斜场景
​				save:
​					saveAsTextFile() : 数据保存到文件中
​				foreach(func: RDD元素类型 => Unit ):Unit
​					foreach里面的函数是针对RDD每个元素操作
​					foreach没有返回值
​				foreachPartition(func: 迭代器[RDD元素类型]=> Unit):Unit  ******
​					foreach与foreachPartition的区别:
​						foreach里面的函数是针对RDD每个元素操作
​						foreachPartition是针对RDD每个分区的所有数据的迭代器操作
​			3.RDD序列化
​				原因:Spark算子里面函数在Executor中执行,算子外面的代码在Driver中执行,有时候Spark算子里面的函数会用到Driver中的对象,所以需要将对象序列化之后传到Executor中使用
​				spark序列化有两种方式: 
​					java序列化【spark默认使用】
​					kryo序列化
​				kryo序列化的性能要比java序列化高10倍左右,工作中一般使用kryo序列化
​				如何使用kryo序列化:
​					1.配置spark序列化方式: new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
​					2.注册类使用kryo序列化[可选]: new SparkConf().registerKryoClasses(Array(classOf[类名],..))
​						注册类与不注册的区别:
​							注册之后序列化的时候全类名不会序列化进去,不注册序列化的时候全类名会序列化进去。如果要序列化的类很多,此时不推荐注册
​			4.依赖关系
​				1.查看血缘[一个Job所有RDD的关系]: rdd.toDebugString
​				2.查看依赖[父子RDD的关系]: rdd.denpendency
​				spark的依赖分为两种: 
​					宽依赖: 有shuffle的称之为宽依赖[父RDD的一个分区被子RDD多个分区所使用的]
​					窄依赖: 没有shuffle的称之为窄依赖[父RDD的一个分区只被子RDD一个分区所使用的]
​				Application: 应用 【一个sparkcontext称为一个Application】
​					job： 任务 【一个action算子产生一个Job】
​						stage: 阶段[ 一个job中stage的个数 = shuffle个数+1 ]
​							task: 一个stage的task个数 = stage中最后一个RDD的分区数
​				一个Application对应多个job
​				一个job对应多个stage【一个Application中多个job是串行】
​				一个stage对应多个task 【一个stage中多个task是并行】
​				后续spark的job划分stage就是根据宽依赖进行划分
​				spark对一个job划分stage的时候是从最后RDD根据依赖关系找到父RDD,如果当前RDD与父RDD的依赖是宽依赖则划分stage,如果是窄依赖则根据父RDD的依赖关系继续找父RDD,在判断依赖关系,循环往复一致找到第一个RDD为止。
​				spark一个job多个stage执行的时候是从前往后执行
​			5.RDD持久化
​				原因: RDD中不存储数据,如果RDD重复使用的话,此时该RDD之前的数据处理逻辑会执行多次,所以如果将RDD的数据持久化之后,后续JOB在执行的时候就可以直接使用持久化的数据,而不用重新计算得到数据。
​				场景: RDD重复使用的
​				RDD持久化分为两种:
​					缓存：
​						存储位置: 存储在task所在主机的内存/本地磁盘中
​						使用:
​							rdd.cache/rdd.persist(存储级别)
​						cache与persist的区别:
​							cache是直接将数据保存在内存中,如果数据量比较大,内存放不下会出现内存溢出
​							persist是可以指定存储级别.
​						常用的存储级别: MEMORY_ONLY[数据只保存在内存中,只用于小数据量场景].MEMORY_AND_DISK[数据首先保存在内存中,如果内存空间不足会一部分数据保存在磁盘,用于大数据量场景]
​					checkpoint
​						原因: 缓存是将数据保存在task所在主机的内存/本地磁盘中,如果task所在主机宕机，数据丢失,此时必须根据依赖关系重新计算得到数据。所以需要将数据持久化到可靠存储介质中
​						存储位置: HDFS中
​						使用:
​							1.设置数据存储路径: sc.setCheckpointDir("...")
​							2.持久化: rdd.checkpoint
​							checkpoint会触发一次job操作,该job操作是在用户第一个Job执行完成之后才会触发。所以对于checkpoint之前的数据处理就会执行两次[第一个job会执行一次,checkpoint产生的job又会执行一次]
​							所以为了减少checkpoint之前的数据处理次数,此时可以与缓存搭配一起使用:
​								rdd.cache
​								rdd.checkpoint
​					缓存与checkpoint的区别:
​						1.数据存储位置不一样:
​							缓存是将数据存储在task所在主机的内存/本地磁盘中
​							checkpoint是将数据存储在HDFS中
​						2.依赖关系是否保留不一样:
​							缓存是将数据存储在task所在主机的内存/本地磁盘中，如果服务器宕机,此时只能根据依赖关系重新计算才能得到数据,所以依赖关系保留
​							checkpoint是将数据保存在HDFS中,数据不会丢失,所以RDD的依赖关系就可以切除了
​			6.分区器
​				spark自带的分区器有两种: HashPartitioner.RangePartitioner
​					HashPartitioner的分区规则: key.hashCode % 分区数 < 0 ? 分区数+ key.hashCode % 分区数 : key.hashCode % 分区数
​					RangePartitioner的分区规则: 如果RDD有N个分区,此时它会通过采样确定N-1个数据,从而确定N个分区的边界,后续通过key与分区的边界比较,从而得知key应该放入哪个分区
​						比如: val rdd2 = rdd1.partitionBy(new RangePartitioner(4,rdd1))
​							此时代表rdd2会有4个分区,此时RangePartitioner通过采样会确定3个key["aa","hh","pp"]
​								此时RDD2 0号分区放的是key<="aa"的数据
​								此时RDD2 1号分区放的是"aa"<key<="hh"的数据
​								此时RDD2 2号分区放的是"hh"<key<="pp"的数据
​								此时RDD2 3号分区放的是"pp"<key的数据
​			7.数据读取与保存
​				读取数据: 
​					sc.textFile()/sc.sequenceFile()/sc.objectFile
​				写入:
​					rdd.saveAsTextFile()/rdd.saveAsSequenceFile()/rdd.saveAsObjectFile()

# 3.累加器 *******

​	场景: 累加器能够一定程度上减少shuffle,用于聚合场景
​	原理: 首先在每个task中累加,然后将task累加的结果发送给Driver,由Driver汇总结果
​	自定义累加器:
​		1.定义一个class继承AccumulatorV2[IN,OUT]
​			IN: 累加的元素类型
​			OUT: 累加器最终结果类型
​		2.重写抽象方法
​			isZero: 累加器是否为空
​			copy: 复制累加器
​			reset: 重置累加器
​			add: 累加元素,在task中执行
​			merge: 汇总task的结果,在Driver中执行
​			value: 获取最终结果
​		3.创建自定义分区器对象: val acc = new 类名
​		4.注册累加器: sc.register(累加器对象)
​		5.累加元素与得到最终结果: acc.add/ acc.value

# 4.广播变量 *******

​	原因: 如果task需要Driver的数据,此时默认情况下会将Driver的数据给每个task都发送一份,所以此时该数据在任务执行过程中占用的内存大小 = task的个数 * 数据的大小,此时占用内存空间相对比较大.
​		  针对该情况,spark提供了广播变量的机制,将数据广播出去之后,此时数据占用的空间 = executor的个数 * 数据的大小 
​	场景:
​		1.大表join小表,此时将小表的数据通过collect收集之后广播出去,可以减少shuffle次数
​		2.task需要Driver数据的时候,将该数据广播出去能够减少数据的空间占用
​	如何使用:
​		1.广播数据: val bc = sc.broadcast(数据)
​		2.task取出数据使用: bc.value