package com.sc.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.sc.gmall.common.constant.GmallConstant
import com.sc.gmall.realtime.bean.StartUpLog
import com.sc.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Autor sc
  * @DATE 0004 16:20
  */
object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP,ssc)

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
    startUpLogStream.foreachRDD{
      rdd =>

        rdd.foreachPartition{startUpLogItr=>
          val jedis = RedisUtil.getJedisClient
          for(startUpLog <-startUpLogItr ){
            val key = "dau" + startUpLog.logDate
            val value = startUpLog.mid
            jedis.sadd(key,value)
          }
          jedis.close()
        }


    }

    //保存到redis中
    //利用redis进行去重过滤



    ssc.start()
    ssc.awaitTermination()

  }

}
