package com.sc.gmall.realtime.bean

/**
  * @Autor sc
  * @DATE 0004 17:22
  */
case class StartUpLog(mid:String,
                      uid:String,
                      appid:String,
                      area:String,
                      os:String,
                      ch:String,
                      logType:String,
                      vs:String,
                      var logDate:String,
                      var logHour:String,
                      var logHourMinute:String,
                      var ts:Long
                     ) {

}
