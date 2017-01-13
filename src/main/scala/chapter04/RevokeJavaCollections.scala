package chapter04

import java.util

import scala.collection.JavaConversions._

/**
  * Created by Administra{
  def getList = {
    val list = new util.ArrayList[String]()
    list.add("摇摆少年梦")
    list.add("学海无涯苦作舟")
    list
  }

  def main(arg: Array[String]): Unit ={
    val list = getList
    list.foreach(println)

    val list2 = list.map(x => x*2)
    println(list2)


  }
}
tor on 2016/8/23 0023.
  */
object RevokeJavaCollections