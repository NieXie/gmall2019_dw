package com.atguigu.gmall2019.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall2019.common.GmallConstants
import com.atguigu.gmall2019.realtime.bean.StartUpLog
import com.atguigu.gmall2019.realtime.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._

object DauApp {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val sparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")
    // 创建SparkContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    // 获取Kafka的输入流
    val inputDstream = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

//    inputDstream.foreachRDD{ rdd=>
//     println(rdd.map(_.value()).collect().mkString("\n"))
//    }
    // 统计日活
    // 转换一下类型 case class 补充两个日期

    val startupLogDstream = inputDstream.map { record =>
      val startupJsonString = record.value()
      val startupLog = JSON.parseObject(startupJsonString, classOf[StartUpLog])
      val datetimeString = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(startupLog.ts))
      startupLog.logDate = datetimeString.split(" ")(0)
      startupLog.logHour = datetimeString.split(" ")(1)
      startupLog
    }

    //  去重 mid  记录每天访问过得mid 形成一个清单

    val filteredDstream: DStream[StartUpLog] = startupLogDstream.transform { rdd =>
      //  driver
      // 利用清单进行过滤 去重
      println("过滤前："+rdd.count())
      val jedis: Jedis = new Jedis("hadoop102", 6379) //driver 每个执行周期查询redis获得清单 通过广播变量发送到executor中
      val dauKey = "dau:" + new SimpleDateFormat("yyyy-MM-dd").format(new Date());
      val dauSet: util.Set[String] = jedis.smembers(dauKey)
      val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
      jedis.close()
      val filteredRDD: RDD[StartUpLog] = rdd.filter { startupLog => //executor
        !dauBC.value.contains(startupLog.mid)
      }
      println("过滤后： "+filteredRDD.count())
      filteredRDD
    }

    //  批次内进行去重 ：  安装key 进行分组 每组取一个
    val groupbyMidDstream: DStream[(String, Iterable[StartUpLog])] = filteredDstream.map(startuplog=>(startuplog.mid,startuplog)).groupByKey()
    val realFilteredDStream: DStream[ StartUpLog ] = groupbyMidDstream.flatMap { case (mid, startlogItr) =>
      startlogItr.take(1)
    }
    realFilteredDStream.cache()


    // 更新清单 存储到redis
    realFilteredDStream.foreachRDD{ rdd=>
     rdd.foreachPartition{ startuplogltr=>
       val jedis = new Jedis("hadoop102",6379)
       for ( startuplog <- startuplogltr) {
         val dauKey = "dau:"+startuplog.logDate
         println(dauKey+":::"+startuplog.mid)
         jedis.sadd(dauKey,startuplog.mid)
       }
       jedis.close()
     }
    }

    // 将数据保存到Phoenix
    realFilteredDStream.foreachRDD{rdd=>

      rdd.saveToPhoenix("gmall2019_dau",Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        new Configuration,Some("hadoop102,hadoop103,hadoop104"))
    }




    // 利用清单进行过滤，去重

    println("启动流程")
    ssc.start()
    ssc.awaitTermination()
  }
}
