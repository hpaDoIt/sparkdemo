package chapter05

/**
  * 第5章 类
  * 5.1 简单类和无参方法
  * 5.2 带getter和setter的属性
  * 5.3 对象私有字段
  * 5.4 Bean属性
  * 5.5 辅助构造器
  * 5.6 主构造器
  * 5.7 嵌套类
  *
  * Created by hpa on 2016/9/1 0001.
  */
object Demo_5_1 {
  def main(args: Array[String]): Unit = {
    println("hello world!!!")

    /**
      * 5.1 简单类和无参方法
      * 在Scala中，类并不声明为public。Scala源文件可以包含多个类，所有这些都具有共有可见性。
      * 创建对象，按照通常的方式来调用方法
      */

    val counter = new Counter

    /**
      * 对于改值器方法使用(),而对于取值器方法去掉()是个不错的风格
      */
    counter.increment()
    //调用无参方法时，你可以写上圆括号，也可以不写
    println(counter.current)

  }
}

class Counter{
  private var value = 0 //你必须初始化字段

  /**
    * 方法默认是公有的
    */
  def increment(){
    value += 1
  }

  def current() = value

  /**
    * 可以通过不带()的方式声明current来强制这种风格
    * 这样一来，就必须用myCounter.current2，不带圆括号。
    */
  def current2 = value
}