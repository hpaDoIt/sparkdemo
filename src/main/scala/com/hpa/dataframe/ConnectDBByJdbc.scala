package com.hpa.dataframe

import java.util.Properties

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

/**
  * Created by Administrator on 2016/12/6.
  */
object ConnectDBByJdbc {
  def main(args : Array[String] ){

    val spark = SparkSession.builder().appName("ConnectDBByJdbc").
      master("local[2]").getOrCreate()

    val jdbcDF = spark.read.format("jdbc").
      option("url", "jdbc:mysql://192.168.0.2:3306/spark").
      option("driver", "com.mysql.jdbc.Driver").
      option("dbtable", "student").
      option("user", "root").
      option("password", "123456").load()

    jdbcDF.show()

    /**
      * 向数据库写入数据
      */
    val studentRDD = spark.sparkContext.
      parallelize(Array("3 Rongcheng M 26", "4 Guanhua M 27")).
      map(_.split(" "))

    /**
      * 设置模式信息
      */
    val schema = StructType(List(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("gender", StringType, true),
      StructField("age", IntegerType, true)))

    /**
      * 创建Row对象，每个Row对象都是rowRDD中的一行
      */
    val rowRDD = studentRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).trim, p(3).toInt))

    /**
      * 建立Row对象和模式之间的对应关系，即把数据和模式对应起来
      */
    val studentDF = spark.createDataFrame(rowRDD, schema)

    /**
      * 创建一个prop常量用来保存JDBC连接参数
      */
    val prop = new Properties();
    prop.put("user", "root")
    prop.put("password", "123456")
    prop.put("driver", "com.mysql.jdbc.Driver")

    /**
      * 连接数据库，采用append模式，表示追加记录到数据库spark的student表中
      */
    studentDF.write.mode("append").
      jdbc("jdbc:mysql://192.168.0.2:3306/spark", "spark.student", prop)

  }
}
