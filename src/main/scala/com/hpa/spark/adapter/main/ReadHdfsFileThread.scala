package com.hpa.spark.adapter.main

import java.io.{BufferedReader, InputStreamReader}
import java.net.URI

import com.hpa.spark.adapter.bean.{BaseConfig, KafkaConfig}
import com.hpa.spark.adapter.kafka.producer.KafkaProduceMsg
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

import scala.util.control.NonFatal

/**
  * 读取指定hdfs上单个文件的线程类
  * Created by hpa on 2017/2/9.
  */
class ReadHdfsFileThread(config: KafkaConfig, readHdfsFileBasePath:String, fileName:String, charsetName:String) extends Runnable{

  override def run(): Unit = {
    println(s"开始读取$readHdfsFileBasePath/$fileName ... ...")
    val produceMsg = new KafkaProduceMsg(config.metadataBrokerList, config.topics)
    val beginTime = System.currentTimeMillis();
    var fis: FSDataInputStream = null
    var bis: BufferedReader = null
    var fs: FileSystem = null
    val hdfsFilePath = readHdfsFileBasePath + "/" + fileName
    try{
      val conf = new Configuration();
      conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
      /*conf.set("fs.defaultFS", readHdfsFileBasePath)
      fs = FileSystem.get(conf)*/

      fs = FileSystem.get(URI.create(readHdfsFileBasePath), conf)

      fis = fs.open(new Path(hdfsFilePath))
      bis = new BufferedReader(new InputStreamReader(fis, charsetName))
      var line = bis.readLine()
      while(line != null){
        if(line != null && !"".equals(line.trim)){
          //println(line)
          produceMsg.sendMsg(line)
          line = bis.readLine()
        }
      }
      val endTime = System.currentTimeMillis();
      println(s"####################fileName: $fileName 读取完毕！总计耗时:" + ( endTime - beginTime)/1000 + "s.#######################")
    }catch {
      case NonFatal(ex) => println(s"Non fatal exception! $ex")
    }finally {
      println("发送完毕!")
      try{
        if(bis != null){
          bis.close()
        }
        if(fis != null){
          fis.close()
        }

        //Non fatal exception! java.io.IOException: Filesystem closed
        /*if(fs != null){
          fs.close()
        }*/
        if(produceMsg != null){
          produceMsg.closeProducer()
        }
      }catch {
        case e: Throwable => println("关闭流异常！")
      }
    }
  }
}