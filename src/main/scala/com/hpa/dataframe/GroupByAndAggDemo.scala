package com.hpa.dataframe

import org.apache.spark.sql.SparkSession

/**
  * Created by hpa on 2016/12/8.
  */
object GroupByAndAggDemo {
  def main(args: Array[String]){
    val sqlContext = SparkSession.builder().
      appName("GroupByAndAggDemo").
      master("local[2]").getOrCreate().sqlContext

    val people = sqlContext.read.
      json("hdfs://192.168.1.210:9000/library/SparkSQL/Data/people.json")

    val newPeople = sqlContext.read.
      json("hdfs://192.168.1.210:9000/library/SparkSQL/Data/newPeople.json")

    val department = sqlContext.read.
      json("hdfs://192.168.1.210:9000/library/SparkSQL/Data/department.json")

    val rnDepartment = department.withColumnRenamed("depId", "id")

    val joinPeople = people.join(rnDepartment,people("depId") === rnDepartment("id"), "outer")

    joinPeople.show

    val joinPeopleAgg = joinPeople.groupBy(department("name")).agg(Map("age" -> "max","gender" -> "count"))

    joinPeopleAgg.show()

    people.printSchema()
    newPeople.printSchema()
    department.printSchema()
    joinPeople.printSchema()

    //department.write.saveAsTable("department")

    val rnPeople = people.withColumnRenamed("job number", "id")
    //rnPeople.write.save("hdfs://192.168.1.210:9000/library/SparkSQL/Data/peopleSave.parquet")
    //rnPeople.write.json("hdfs://192.168.1.210:9000/library/SparkSQL/Data/peopleJson.json");
    //rnPeople.write.parquet("hdfs://192.168.1.210:9000/library/SparkSQL/Data/peopleParquet.parquet")

    /**
      * 向text文件中写数据，dataFrame只能有一个column，不能有多个columns
      */
    rnPeople.select("name").write.text("hdfs://192.168.1.210:9000/library/SparkSQL/Data/peopleText.txt")
    /**
      * +------------------+--------+-------------+
        |  Development Dept|      33|            2|
        |Testing Department|      26|            2|
        |    Personnel Dept|      30|            2|
      */


    /**
      * Exception in thread "main" org.apache.hadoop.security.AccessControlException: Permission denied: user=Administrator, access=WRITE, inode="/library/SparkSQL/Data":sparkadmin:supergroup:drwxr-xr-x
      * 解决办法：
      * 在hdfs-site.xml加入如下代码
      * <property>
      *   <name>dfs.permissions</name>
      *   <value>false</value>
      *   <description>
      *     If "true", enable permission checking in HDFS.
      *     If "false", permission checking is turned off,
      *     but all other behavior is unchanged.
      *     Switching from one parameter value to the other does not change the mode,
      *     owner or group of files or directories.
      *     </description>
      * </property>
      */

  }
}
