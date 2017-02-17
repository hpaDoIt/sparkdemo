package com.hpa.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2016/12/17.
  */
object SparkStreamingWordCount {
  def main(args: Array[String]){
    /**
      *
      * 配置应用名称以及配置两个线程（注意要大于等于2个线程）
      */
    val conf = new SparkConf().
      setAppName("KafkaStreaming2HDFS")


    /**
      * 创建StreamingContext
      * 这个是SparkStreaming一切的开始
      * 可以基于SparkConf参数，也可以基于持久化的SparkStreaming进行状态恢复。
      * 典型的场景是Driver崩溃后由于Spark Streaming具有连续不断的24小时不间断的运行，
      * 所以需要在Driver重新启动后从上次运行的状态恢复过来，此时的状态需要基于曾经的CheckPoint。
      */
    val ssc = new StreamingContext(conf, Seconds(30))

    /**
      * 可以设置数据的来源
      * 这里来源于socket，设置端口为8888，数据存储级别为MEMORY_AND_DISK
      */
    val receiverInputDStream = ssc.socketTextStream("10.1.235.47", 8888, StorageLevel.MEMORY_AND_DISK)

    /**
      * 对socket方式接收的数据进行操作
      * 使用空格" "将每条数据切分成一个一个单词
      * (line1,line2)
      * (word11,word12,line21,line22)
      */
    val tuple = receiverInputDStream.flatMap(_.split(" "))

    /**
      * map：将每个单词计数为1
      * reduceByKey：将相同单词的个数想家，从而得到总的单词个数
      * for(item: List)
      * ((word11,1),(word12,1),(line21,1),(line22,1))
      */
    val tupleCount = tuple.map(word => (word, 1)).reduceByKey(_ + _)

    tupleCount.print()

    /**
      * 上面的print方法不会触发job执行，
      * 因为目前代码处于Spark Streming框架的控制之下，
      * 具体是否触发job是取决于设置的Duration时间的间隔
      */
    ssc.start

    /**
      * 等待程序结束
      */
    ssc.awaitTermination()
  }
}
