package com.hpa.spark.bean

/**
  * S11信令信息（手机、CPE等上网设备实时位置信息）实体类
  * Created by hpa on 2017/2/9.
  */
case class Ods28Ltec1S1mmHm(imsi: String, msisdn: String, tac: String, eci: String, apn: String)
