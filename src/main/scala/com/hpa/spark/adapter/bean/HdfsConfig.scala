package com.hpa.spark.adapter.bean

/**
  * HDFS数据源配置类
  * Created by hpa on 2017/2/9.
  */
case class HdfsConfig(hdfsIp: String, hdfsPort: String, hdfsFileBasePath: String) extends BaseConfig
