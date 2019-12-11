package com.sc.gmall.exportsale.app

import com.sc.gmall.common.constant.GmallConstant
import com.sc.gmall.common.util.MyEsUtil
import com.sc.gmall.exportsale.bean.SaleDetailDaycount
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object ExportSaleApp {

  def main(args: Array[String]): Unit = {
    var date = ""
    if (args != null && args.length > 0) {
      date = args(0)
    } else {
      date = "2019-12-09"
    }


    val sparkConf: SparkConf = new SparkConf().setAppName("sale_app").setMaster("local[*]")

    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    sparkSession.sql("use gmall")
    import sparkSession.implicits._
    val saleDetailDaycountRDD: RDD[SaleDetailDaycount] = sparkSession.sql("select user_id,sku_id,user_gender,cast(user_age as int) user_age,user_level,cast(sku_price as double),sku_name,sku_tm_id, sku_category3_id,sku_category2_id,sku_category1_id,sku_category3_name,sku_category2_name,sku_category1_name,spu_id,sku_num,cast(order_count as bigint) order_count,cast(order_amount as double) order_amount,dt" +
      " from dws_sale_detail_daycount where dt='" + date + "'").as[SaleDetailDaycount].rdd


    saleDetailDaycountRDD.foreachPartition { saleItr =>
      val listBuffer: ListBuffer[SaleDetailDaycount] = ListBuffer()
      for (saleDetail <- saleItr) {
        listBuffer += saleDetail
        if (listBuffer.size == 100) {
          MyEsUtil.indexBulk(GmallConstant.ES_INDEX_SALE_DETAIL, listBuffer.toList)
          listBuffer.clear()
        }
      }
      if (listBuffer.size > 0) {
        MyEsUtil.indexBulk(GmallConstant.ES_INDEX_SALE_DETAIL, listBuffer.toList)
      }

    }


  }

}
