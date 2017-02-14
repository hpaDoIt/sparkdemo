package com.hpa.spark.cep.utils

import java.sql.{Connection, DriverManager}

import com.hpa.spark.cep.bean.DBConfig

/**
  * Created by hpa on 2017/2/10.
  */
object DataSourceUtil {
  var connection: Connection = null


  def getConnection(dbConfig: DBConfig): Connection ={
    DriverManager.getConnection(s"${dbConfig.url}", dbConfig.username, dbConfig.password)
  }

}
