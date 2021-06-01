import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat

/**
 * @author Grant
 * @create 2021-05-31 16:41
 */
object TitleUserAction {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("xxx")
    val sc = new SparkContext(conf)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val list = List[(String,String,String)](
      ("1001","2020-09-10 10:21:21","home.html"),
      ("1001","2020-09-10 10:28:10","good_list.html"),
      ("1001","2020-09-10 10:35:05","good_detail.html"),
      ("1001","2020-09-10 10:42:55","cart.html"),
      ("1001","2020-09-10 11:35:21","home.html"),
      ("1001","2020-09-10 11:36:10","cart.html"),
      ("1001","2020-09-10 11:38:12","trade.html"),
      ("1001","2020-09-10 11:40:00","payment.html"),
      ("1002","2020-09-10 09:40:00","home.html"),
      ("1002","2020-09-10 09:41:00","mine.html"),
      ("1002","2020-09-10 09:42:00","favor.html"),
      ("1003","2020-09-10 13:10:00","home.html"),
      ("1003","2020-09-10 13:15:00","search.html")
    )

    val value = sc.parallelize(List(list))
      .flatMap(x => x)
    val value1 = value
      .groupBy(_._1)
    value1
      .map(x=>{
        val list1 = x._2.toList
        (x._1,list1)
      })
      .map(x=>{
        val value2 = x._2
        val tuples = value2.sortBy(_._2)
        (x._1,tuples)
      })
      .map(x=>{
        val value2 = x._2
        val from = dateFormat.parse(value2(0)._2).getTime
        val tuples = value2.map(y => {
          val to = dateFormat.parse(y._2).getTime
          val l = (to - from) / (1000 * 60)
          var flag = 0
          if (l <= 30) flag
          else flag = ((l - 30) / 30 + 1).toInt
          (y._1, y._2,y._3,flag+y._1)
        })




        tuples
      }).flatMap(x=>x)
      .groupBy(_._4)
      .map(x=>{
        val list1 = x._2.toList
        val tuples = list1.map(x => {
          (x._1, x._2, x._3, x._4, list1.indexOf(x)+1)
        })
        (x._1,tuples)

      })

      .foreach(println)

    sc.stop()
  }
}
