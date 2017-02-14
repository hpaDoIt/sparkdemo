package com.hpa.spark.adapter.main

import java.io.File
import java.net.URI

import akka.actor._
import akka.util.Timeout
import com.hpa.spark.adapter.bean.{HdfsConfig, KafkaConfig}
import com.typesafe.config.{ConfigFactory, ConfigObject}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import scala.concurrent.duration._

/**
  * 定时扫描指定HDFS目录下所有文件，并发送到Kafka的指定topic中
  * Created by hpa on 2017/2/9.
  */
object Bootstrap {
  val actorSystem = ActorSystem("Bootstrap")
  val act = actorSystem.actorOf(Props[MyActor] ,"first")
  implicit val time = Timeout(10 minute)
  var kafkaConfig: KafkaConfig = null
  var hdfsConfig: HdfsConfig = null

  def loadConfig0() = {
    println("开始初始化数据源输入输出配置... ...")
    val config = ConfigFactory.load("application")
    kafkaConfig = KafkaConfig(config.getString("sourceConfig.items.output.kafka.metadataBrokerList"),
      config.getString("sourceConfig.items.output.kafka.groupId"),
      config.getString("sourceConfig.items.output.kafka.topics"),
      config.getString("sourceConfig.items.output.kafka.requestRequiredAcks"),
      config.getString("sourceConfig.items.output.kafka.producerType"))

    kafkaConfig.charsetName = config.getString("sourceConfig.items.output.kafka.charsetName")

    hdfsConfig = HdfsConfig(
      config.getString("sourceConfig.items.input.hdfs.hdfsIp"),
      config.getString("sourceConfig.items.input.hdfs.hdfsIp"),
      config.getString("sourceConfig.items.input.hdfs.hdfsFileBasePath")
    )
    println("初始化数据源输入输出配置成功!!!")
  }

  /**
    * 初始化数据源输入输出配置
    */
  def loadConfig() = {
    println("开始初始化数据源输入输出配置... ...")
    val kafkaObj = actorSystem.settings.config.getObject("sourceConfig.items.output.kafka")
    val hdfsObj = actorSystem.settings.config.getObject("sourceConfig.items.input.hdfs")

    kafkaConfig = KafkaConfig(kafkaObj.get("metadataBrokerList").render().replace("\"",""),
      kafkaObj.get("groupId").render().replace("\"",""),
      kafkaObj.get("topics").render().replace("\"",""),
      kafkaObj.get("requestRequiredAcks").render().replace("\"",""),
      kafkaObj.get("producerType").render().replace("\"",""))
    kafkaConfig.charsetName = kafkaObj.get("charsetName").render().replace("\"","")

    hdfsConfig = HdfsConfig(
      hdfsObj.get("hdfsIp").render.replace("\"",""),
      hdfsObj.get("hdfsPort").render.replace("\"",""),
      hdfsObj.get("hdfsFileBasePath").render.replace("\"","")
    )
    println("初始化数据源输入输出配置成功!!!")
  }

  def main(args: Array[String]): Unit = {

    //初始化数据源输入输出配置
    loadConfig()

    val readHdfsFileBasePath = s"hdfs://${hdfsConfig.hdfsIp}:${hdfsConfig.hdfsPort}${hdfsConfig.hdfsFileBasePath}"
    val defaultFS = s"hdfs://${hdfsConfig.hdfsIp}:${hdfsConfig.hdfsPort}"
    val conf = new Configuration()
    conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    //conf.set("fs.defaultFS", readHdfsFileBasePath)
    val fs = FileSystem.get(new URI(readHdfsFileBasePath),conf)
    if(!fs.exists(new Path(readHdfsFileBasePath))){
      println(s"不存在$readHdfsFileBasePath 目录！")
      System.exit(-1)
    }
    val fileList:Array[FileStatus] = fs.listStatus(new Path(readHdfsFileBasePath))
    var fileName = ""
    //var hdfsFilePath = ""
    try{
      for(item <- fileList){
        fileName = item.getPath.getName
        //hdfsFilePath = readHdfsFileBasePath + "/" + fileName
        import scala.concurrent.ExecutionContext.Implicits.global
        actorSystem.scheduler.schedule(0 milliseconds, 1 seconds, new ReadHdfsFileThread(kafkaConfig, readHdfsFileBasePath, fileName , kafkaConfig.charsetName) )
      }
    }catch {
      case e: Throwable => println(e)
    }finally {
      if(fs != null){
        fs.close()
      }
    }

  }
}

/**
  * 测试数据
开始读取hdfs://10.1.235.51:8020/testdata/H110001201605280932009097771.AVL(文件大小：420 M,或者430303 k) ... ...
开始读取hdfs://10.1.235.51:8020/testdata/H110001201605280931009449863.AVL(文件大小：550 M,或者564030 k) ... ...
####################fileName: H110001201605280932009097771.AVL 读取完毕！总计耗时:103s(读取速度4177.7 k/s).#######################
发送完毕!
####################fileName: H110001201605280931009449863.AVL 读取完毕！总计耗时:131s(读取速度4305.6 k/s).#######################
发送完毕!
  */
