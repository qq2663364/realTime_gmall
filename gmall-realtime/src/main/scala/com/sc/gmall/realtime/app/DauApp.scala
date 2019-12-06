package com.sc.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.sc.gmall.common.constant.GmallConstant
import com.sc.gmall.common.util.MyEsUtil
import com.sc.gmall.realtime.bean.StartUpLog
import com.sc.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * @Autor sc
  * @DATE 0004 16:20
  */
object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP, ssc)

    val startUpLogStream: DStream[StartUpLog] = inputDstream.map {
      record =>
        val jsonStr: String = record.value()
        val startUpLog: StartUpLog = JSON.parseObject(jsonStr, classOf[StartUpLog])
        val date = new Date(startUpLog.ts)
        val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(date)
        val dateArr: Array[String] = dateStr.split(" ")
        startUpLog.logDate = dateArr(0)
        startUpLog.logHour = dateArr(1).split(":")(0)
        startUpLog.logHourMinute == dateArr(1)
        startUpLog
    }

    //利用transform去重
    val filterDStream: DStream[StartUpLog] = startUpLogStream.transform { rdd =>
      println("过滤前" + rdd.count())

      //driver上执行 ,周期性执行
      //利用redis进行去重过滤,会用广播变量,避免频繁getJedisClient
      val curdate: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val jedis: Jedis = RedisUtil.getJedisClient
      val key = "dau" + curdate
      //获取当天用户的key
      val dauSet: util.Set[String] = jedis.smembers(key)
      //把dauSet放入广播变量
      val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
      //executor上执行
      val filterRDD = rdd.filter {
        startUpLog =>
          val dauSet: util.Set[String] = dauBC.value
          !dauSet.contains(startUpLog.mid)
      }
      println("过滤后:" + filterRDD.count())
      filterRDD
    }

    //二次去重思路
    val groupByMidDStream: DStream[(String, Iterable[StartUpLog])] = filterDStream.map(startUpLog => (startUpLog.mid, startUpLog)).groupByKey()
    val distinctDStream: DStream[StartUpLog] = groupByMidDStream.flatMap { case (mid, startLogItr) =>
      startLogItr.take(1)
    }


    //保存到redis中
    distinctDStream.foreachRDD { rdd =>
      rdd.foreachPartition { startUpLogItr =>
        val list: List[StartUpLog] = startUpLogItr.toList
        val jedis = RedisUtil.getJedisClient
        for (startUpLog <- list) {
          val key = "dau" + startUpLog.logDate
          val value = startUpLog.mid
          jedis.sadd(key, value)
        }
        MyEsUtil.indexBulk(GmallConstant.ES_INDEX_DAU,list)
        jedis.close()
      }
    }


    ssc.start()
    ssc.awaitTermination()

  }

}
