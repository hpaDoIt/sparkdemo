package chapter03

import scala.collection.mutable.ArrayBuffer

/**
  * Created by hpa on 2016/8/24 0024.
  */
object Array_ArrayBuffer_Exercise {
  def main(args: Array[String]): Unit ={
    /**
      * 3.3 数组遍历
      */
    val b = ArrayBuffer(1,2,3,6)
    val a = b.toArray
    for(i <- 0 until b.length)
      println(i + ": " + a(i))

    println(0 until 10)
    println(0.until(10))

    //每两个元素一跳
    println(0 until (10,2))

    //从数组的尾端开始遍历
    println((0 until 10).reverse)

    //不用数组下标也可以直接遍历数组
    for(item <- a)
      println(item)

    /**
      * 3.4 数组转换
      * 从一个数组或者数组缓冲出发，以某种方式对它进行转换是很简单的。
      * 这些转换动作不会修改原始数组，而是产生一个全新的数组。
      */
    val array = Array(2,3,5,7,11)
    val result = for(elem <- array) yield 2 * elem
    for(elem <- result)
      println(elem)

    //注意：从数组出发，得到的是另一个数组；从数组缓冲出发，得到的是另一个数组缓冲。

    //当遍历一个集合时，如果只想处理那些满足特定条件的元素，可以通过守卫：for中if来实现。
    val partOfArray1 = for(elem <- array if elem % 2 == 0) yield 2 * elem
    println("#####################################")
    for(elem <- partOfArray1)
      println(elem)

    //另一种方法
    val partOfArray2 = array.filter(_ % 2 == 0).map(2 * _)
    //或者
    val partOfArray3 = array.filter{_ % 2 == 0} map {2 * _}

    println("#####################################")
    for(elem <- partOfArray2)
      println(elem)

    println("#####################################")
    for(elem <- partOfArray3)
      println(elem)


    val intArray = Array(1,3,-2,-6,-9,16)
    var first = true
    val indexs = for(i <- 0 until intArray.length if first || intArray(i) > 0) yield {
      if(intArray(i) < 0)
        first = false;
      i
    }
    println("indexs:" + indexs)
    //结果：indexs:Vector(0, 1, 2, 5)

    for(j <- 0 until indexs.length)intArray(j) = intArray(indexs(j))

    for(elem <- intArray)
      println("elem: " + elem)

    /**
      * 结果：
      * elem: 1
      * elem: 3
      * elem: -2
      * elem: 16
      * elem: -9
      * elem: 16
      */
    val lastResult = intArray.toBuffer
    lastResult.trimEnd(intArray.length - indexs.length)
    println("##################################")
    for(elem <- lastResult)
      println(elem)


    /**
      * 3.5 常用算法
      */
    //sum、max、min，要使用sum方法，元素类型必须是数值类型：要么是整型，要么是浮点或者BigInteger/BigDecimal。
    val sum = Array(1,7,2,9).sum
    println("sum = " + sum)

    val max = Array(1,7,2,9).max
    println("max = " + max)

    val maxStr = Array("Mary", "had", "a", "little", "lamb").max
    println("maxStr = " + maxStr)

    val min = Array(1,7,2,9).min
    println("min = " + min)

    //sorted方法将数组和数组缓冲排序并返回经过排序的数组或者数组缓冲，原始版本不改变
    val intArrayBuffer = ArrayBuffer(1,7,2,9)
    val intABSorted = intArrayBuffer.sorted
    for(elem <- intABSorted)
      println(elem)

    val intABSorted2 = intArrayBuffer.sortWith(_ > _)
    for(elem <- intABSorted2)
      println(elem)

    println("###########################")
    //可以直接对一个数组排序，但不能对数组缓冲排序，是对原始版本排序
    val directSortedArray = Array(1,7,2,8)
    scala.util.Sorting.quickSort(directSortedArray)
    for(elem <- directSortedArray)
      println(elem)

    //使用mkString方法来显示数组或者数组缓冲的内容，它允许指定元素之间的分隔符，也可以指定前缀和后缀。
    println(directSortedArray.mkString(" and "))
    println(directSortedArray.mkString("{", ",", "}"))

    //Array.toString调用的是来之Java的toString()方法
    println(directSortedArray.toString)

    //ArrayBuffer.toString方法报告了类型，编译调试
    println(intArrayBuffer.toString)

    //3.7 多维数组
    //如何来构建一个Array[Array[double]]数组？可以用ofDim方法
    val matrix = Array.ofDim[Double](3, 4)//三行四列

    //访问二维数组的元素
    matrix(1)(2)  = 42

    //可以创建不规则的数组，每一行的长度各不相同
    val triangle = new Array[Array[Int]](10)
    for(i <- 0 until triangle.length)
      triangle(i) = new Array[Int](i + 1)

    //遍历不规则二维数组Array
    for(i <- 0 until triangle.length)
      for(j <- 0 until triangle(i).length){
        print(triangle(i)(j) + " ")
        if(j == triangle(i).length - 1)
          println()
      }

    /**
      * 3.8 与Java的互操作
      * 由于Scala数组是用Java数组实现的，你可以在Java和Scala之间来回传递
      */
    //Scala到Java转换
    import scala.collection.JavaConversions.bufferAsJavaList
    val command = ArrayBuffer("ls", "-al", "/home/cay")
    //java.lang.ProcessBuilder类有一个以List<String>为参数的构造器，ArrayBuffer自动转换为List
    val pb = new ProcessBuilder(command)

    /**
      * Java到Scala的转换
      * 当Java方法返回java.util.List时，可以让它自动转换成为一个Buffer。
      */
    import scala.collection.JavaConversions.asScalaBuffer
    import scala.collection.mutable.Buffer
    //不能使用ArrayBuffer，包装起来的对象仅能保证是个Buffer
    val cmd: Buffer[String] = pb.command()
    //如果Java方法返回一个包装过的Scala缓冲，那么隐士转换会将原始的对象解包处理。


  }
}
