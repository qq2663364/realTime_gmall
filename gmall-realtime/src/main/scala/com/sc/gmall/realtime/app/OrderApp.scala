package com.sc.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.sc.gmall.common.constant.GmallConstant
import com.sc.gmall.common.util.MyEsUtil
import com.sc.gmall.realtime.bean.OrderInfo
import com.sc.gmall.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * @Autor sc
  * @DATE 0009 19:43
  */
object OrderApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("OrderApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //保存到ES
    //数据脱敏,补充时间戳
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_NEW_ORDER,ssc)

    val orderInfoDStream: DStream[OrderInfo] = inputDStream.map { record =>
      val jsonStr = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(jsonStr, classOf[OrderInfo])

      //手机号脱敏
      val telSplit: (String, String) = orderInfo.consigneeTel.splitAt(4)
      orderInfo.consigneeTel = telSplit._1 + "******"

      val dateTimeArr: Array[String] = orderInfo.createTime.split(" ")
      orderInfo.createDate = dateTimeArr(0)

      val timeArr: Array[String] = dateTimeArr(1).split(":")

      orderInfo.createHour = timeArr(0)
      orderInfo.createHourMinute = timeArr(0) + ":" + timeArr(1)
      orderInfo
    }

    //增加一个字段0或1 标识该订单是否是该用户首次下单

    //数据写入ES
    orderInfoDStream.foreachRDD{rdd=>
      rdd.foreachPartition{orderItr=>
        MyEsUtil.indexBulk(GmallConstant.ES_INDEX_NEW_ORDER,orderItr.toList)
      }
    }



    ssc.start()
    ssc.awaitTermination()

  }

}
