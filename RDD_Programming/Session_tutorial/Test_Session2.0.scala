import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.UUID

/**
 * @author Grant
 * @create 2021-06-01 18:50
 */

case class UserAnalysis(
                       userId:String,
                       time:Long,
                       page:String,
                       var session:String=UUID.randomUUID().toString,
                       var step:Int=1
                       )
object TestSession {
  def main(args: Array[String]): Unit = {
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

    val conf = new SparkConf().setMaster("local[4]").setAppName("xxx")
    val sc = new SparkContext(conf)

    sc.parallelize(list)
      .map{
        case (userId,timeStr,page) => 
          val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val time = format.parse(timeStr).getTime
          UserAnalysis(userId,time,page)
      }
      .groupBy(_.userId)
      .flatMap(x=>{
        val sortList = x._2.toList.sortBy(_.time)
        val slidingList = sortList.sliding(2)
        slidingList.foreach(windown =>{
          val first = windown.head
          val next = windown.last
          if (next.time-first.time<=30*60*1000){
            next.session = first.session
            next.step=first.step+1
          }
        })
        x._2
      })
      .foreach(println(_))
  }
}