package com.hpa.spark.adapter.bean

/**
  * kafka数据源配置类
  * Created by hpa on 2017/2/9.
  */
case class KafkaConfig(
                        metadataBrokerList: String,
                        groupId: String,
                        topics: String,
                        requestRequiredAcks: String,
                        producerType: String) extends BaseConfig
